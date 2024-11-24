package eslogger

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticsearchSink struct {
	ElasticSearchOpts
	esClient        *elasticsearch.Client
	logBuffer       []map[string]interface{}
	mu              sync.Mutex
	logEntry        chan map[string]interface{}
	flushInProgress bool
}

var flushTimer *time.Ticker

type ElasticSearchOptsFunc func(*ElasticSearchOpts)

func defaultOpts() ElasticSearchOpts {
	indexTemplate := fmt.Sprintf("eslogger-%s", time.Now().Format("2006-01-02"))
	return ElasticSearchOpts{
		Address:                 "http://localhost:9200",
		UserName:                "",
		Password:                "",
		BatchSize:               50,
		FlushIntervalInSeconds:  15,
		IndexTemplate:           indexTemplate,
		IlmPolicy:               fmt.Sprintf("%s-ilm-policy", indexTemplate),
		EnableTls:               false,
		ShouldGenerateIlmPolicy: false,
	}
}

type ElasticSearchOpts struct {
	Address                 string
	UserName                string
	Password                string
	BatchSize               int
	FlushIntervalInSeconds  int
	IndexTemplate           string
	IlmPolicy               string
	EnableTls               bool
	ShouldGenerateIlmPolicy bool
}

func withTls(opts *ElasticSearchOpts) {
	opts.EnableTls = true
}

func withShouldGenerateIlmPolicy(opts *ElasticSearchOpts) {
	opts.ShouldGenerateIlmPolicy = true
}

func withFlushIntervalInSeconds(intervalInSeconds int) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.FlushIntervalInSeconds = intervalInSeconds
	}
}

func withBatchSize(batchSize int) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.BatchSize = batchSize
	}
}

func withAddress(address string) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.Address = address
	}
}

func withIlmPolicy(ilmPolicy string) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.IlmPolicy = ilmPolicy
	}
}

func withUserName(userName string) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.UserName = userName
	}
}
func withPassword(password string) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.Password = password
	}
}

func withIndexTemplate(indexTemplate string) ElasticSearchOptsFunc {
	return func(opts *ElasticSearchOpts) {
		opts.IndexTemplate = indexTemplate
	}
}

func NewEsWriter(opts ...ElasticSearchOptsFunc) (*ElasticsearchSink, error) {

	configOpts := defaultOpts()

	for _, fn := range opts {
		fn(&configOpts)
	}

	esClient, err := elasticClient(&configOpts)
	if err != nil {
		return nil, errors.New("Error creating elasticsearch client")
	}

	if configOpts.ShouldGenerateIlmPolicy {
		err = createIlmPolicy(esClient, configOpts.IlmPolicy)
		if err != nil {
			return nil, errors.New("Error creating ILM policy")
		}
	}

	err = initializeDataStream(configOpts.IndexTemplate, configOpts.IlmPolicy, esClient)
	if err != nil {
		return nil, errors.New("Error creating index template")
	}

	esWriter := &ElasticsearchSink{
		ElasticSearchOpts: configOpts,
		esClient:          esClient,
		logBuffer:         make([]map[string]interface{}, 0, configOpts.BatchSize),
		logEntry:          make(chan map[string]interface{}, configOpts.BatchSize),
	}

	go esWriter.startTimer()

	return esWriter, nil
}

func elasticClient(config *ElasticSearchOpts) (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			config.Address,
		},
		Username: config.UserName,
		Password: config.Password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	esClient, err := elasticsearch.NewClient(cfg)

	if err != nil {
		log.Printf("Error creating the client: %s", err)
		return nil, err
	}

	info, err := esClient.Info()
	if err != nil || info.IsError() {
		log.Printf("Failed to Initialize Elasticsearch %s", err)
		return nil, err
	}

	return esClient, nil
}

func initializeDataStream(indexTemplate string, ilmPolicy string, esClient *elasticsearch.Client) error {

	mapping, err := createComponentTemplate(esClient, indexTemplate)

	if err != nil {
		return errors.New("Error creating Component Template")
	}

	settingsName, err := componentIlmPolicyMapping(esClient, ilmPolicy)

	if err != nil {
		return errors.New("Error creating ILM Mapping Index Template")
	}

	err = createIndexTemplate(indexTemplate, []string{mapping, settingsName}, esClient)
	if err != nil {
		return errors.New("Error creating Index Template")
	}

	err = createDataStream(indexTemplate, esClient)
	if err != nil {
		return errors.New("Error creating data stream")
	}

	return nil
}

func createIlmPolicy(esClient *elasticsearch.Client, ilmPolicy string) error {

	resp, err := esClient.ILM.PutLifecycle(ilmPolicy, esClient.ILM.PutLifecycle.WithBody(strings.NewReader(`{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_primary_shard_size": "50gb"
          }
        }
      },
      "warm": {
        "min_age": "10d",
        "actions": {
          "shrink": {
            "number_of_shards": 1
          },
          "forcemerge": {
            "max_num_segments": 1
          }
        }
      },
      "delete": {
        "min_age": "15d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}`)))

	if err != nil {
		log.Printf("Error getting response: %s", err)
		return err
	}

	fmt.Printf("CreateIlm Response: %s", resp)
	return nil
}

func createComponentTemplate(esClient *elasticsearch.Client, mappingName string) (string, error) {
	payload := strings.NewReader(fmt.Sprintf(`{
		"template": {
			"mappings": {
				"dynamic": true,
				"properties": {
					"@timestamp": {
						"type": "date"
					},
					"message": {
						"type": "text"
					},
					"log.level": {
						"type": "keyword"
					},
					"service.name": {
						"type": "keyword"
					},
					"ecs.version": {
						"type": "keyword"
					}
				}
			}
		},
		"_meta": {
			"description": "Mappings for @timestamp and message fields",
			"my-custom-meta-field": "More arbitrary metadata"
		}
	}`))

	resp, err := esClient.Cluster.PutComponentTemplate(mappingName, payload)

	if err != nil {
		log.Printf("Error getting response: %s", err)
		return "", err
	}

	if resp.IsError() {
		log.Printf("Error: %s", resp.String())
		return "", errors.New("Error creating Component Template")
	}

	//fmt.Printf("Response: %s", resp)

	return mappingName, nil
}

func componentIlmPolicyMapping(esClient *elasticsearch.Client, ilmPolicy string) (string, error) {
	ilmPolicyComponentPayload := strings.NewReader(fmt.Sprintf(`{
        "template": {
            "settings": {
                "index.lifecycle.name": "%s"
            }
        },
        "_meta": {
            "description": "Settings for ILM",
            "my-custom-meta-field": "More arbitrary metadata"
        }
    }`, ilmPolicy))

	settingsName := fmt.Sprintf("%s-setting", ilmPolicy)

	resp, err := esClient.Cluster.PutComponentTemplate(settingsName, ilmPolicyComponentPayload)

	if err != nil {
		log.Printf("Error getting response: %s", err)
		return "", err
	}

	if resp.IsError() {
		log.Printf("Error: %s", resp.String())
		return "", errors.New("Error creating ILM Mapping Index Template")
	}
	//fmt.Printf("Response: %s", resp)
	return settingsName, nil

}

func createIndexTemplate(indexTemplate string, settings []string, esClient *elasticsearch.Client) error {

	settingsJSON, err := json.Marshal(settings)
	if err != nil {
		log.Printf("Error marshalling settings: %s", err)
	}

	indexPatterns := fmt.Sprintf(`"index_patterns": ["%s*"]`, indexTemplate)
	payload := strings.NewReader(fmt.Sprintf(`{
		%s,
		"data_stream": {},
		"priority": 500,
		"composed_of": %s,
		"_meta": {
		  "description": "Template for my time series data",
		  "my-custom-meta-field": "More arbitrary metadata"
		}
	}`, indexPatterns, settingsJSON))

	resp, err := esClient.Indices.PutIndexTemplate(indexTemplate, payload)

	if err != nil {
		log.Printf("Error getting response: %s", err)
		return err
	}

	if resp.IsError() {
		log.Printf("Error: %s", resp.String())
		return errors.New("Error creating Index Template")
	}

	//fmt.Printf("Response: %s", resp)

	return nil
}

func createDataStream(indexTemplate string, esClient *elasticsearch.Client) error {

	dataStream := fmt.Sprintf("%s-", indexTemplate)

	fmt.Println(dataStream)

	options := func(r *esapi.IndicesCreateDataStreamRequest) {
		r.Pretty = true
		r.Human = true
	}

	resp, err := esClient.Indices.CreateDataStream(indexTemplate, options)

	if err != nil {
		log.Printf("Error getting response: %s", err)
		return err
	}

	if resp.IsError() {
		log.Printf("Error: %s", resp.String())

		if strings.Contains(resp.String(), "resource_already_exists_exception") {
			return nil
		}
		return errors.New("Error creating data stream")
	}

	//	fmt.Printf("Response: %s", resp)

	return nil

}

func (w *ElasticsearchSink) Write(data []byte) (n int, err error) {

	var logEntry map[string]interface{}

	dataErr := json.Unmarshal(data, &logEntry)
	if dataErr != nil {
		fmt.Println("Error unmarshaling JSON:", dataErr)
		return
	}

	select {
	case w.logEntry <- logEntry:
	// Successfully queued log entry
	default:
		// Log buffer is full, flush logs directly
		w.flushBufferedLogs()
		w.logEntry <- logEntry
	}

	return len(data), nil
}

func (w *ElasticsearchSink) startTimer() {
	flushTimer = time.NewTicker(time.Duration(w.FlushIntervalInSeconds) * time.Second)
	defer flushTimer.Reset(time.Duration(w.FlushIntervalInSeconds) * time.Second)
	for {
		select {
		case <-flushTimer.C:
			// Time to flush logs
			w.flushBufferedLogs()
			flushTimer.Reset(time.Duration(w.FlushIntervalInSeconds) * time.Second)
		}
	}
}

func (w *ElasticsearchSink) flushBufferedLogs() {
	w.mu.Lock()
	if w.flushInProgress {
		w.mu.Unlock()
		return
	}

	w.flushInProgress = true
	w.mu.Unlock()

	go func() {
		defer func() {
			w.mu.Lock()
			w.flushInProgress = false
			w.mu.Unlock()
		}()

		for {
			select {
			case logEntry := <-w.logEntry:
				w.logBuffer = append(w.logBuffer, logEntry)
				if len(w.logBuffer) >= int(w.BatchSize) {
					w.flushLogs()
				}
			default:
				// If no new log entries, flush the existing ones
				if len(w.logBuffer) > 0 {
					w.flushLogs()
				}
				return
			}
		}
	}()
}

func (w *ElasticsearchSink) flushLogs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	bulkBody := new(bytes.Buffer)
	enc := json.NewEncoder(bulkBody)

	var operations = []interface{}{}

	for _, logEntry := range w.logBuffer {
		// Create the action part of the bulk request
		operations = append(operations, map[string]interface{}{"create": map[string]string{}}, logEntry)
	}

	for _, op := range operations {
		if err := enc.Encode(op); err != nil {
			log.Fatalf("Error encoding operation: %s", err)
		}
	}

	options := func(r *esapi.BulkRequest) {
		r.Pretty = true
		r.Index = w.IndexTemplate
	}

	res, err := w.esClient.Bulk(bytes.NewReader(bulkBody.Bytes()), options)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	fmt.Printf("Bulk response: %s\n", res)

	// Clear the log buffer
	w.logBuffer = w.logBuffer[:0]
}
