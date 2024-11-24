package main

import (
	"errors"
	"fmt"
	"time"

	log "zerolog-sinks-elasticsearch/eslogger"

	"math/rand"
)

// main is an example of how to use the eslogger package.
//
// It first initializes the logger with a log level of Info.
// Then it logs a message with Info level.
// After that, it creates an Employee struct and logs a message with InfofWithProperties,
// where the Employee is attached as a property.
// Finally, it generates a random error and logs it with Error.
func main() {
	_ = log.NewLogger(false)
	lr := log.GetLogger()
	defer handlePanic(lr)
	lr.Info("Hello World")
	lr.Infof("Hello %s", "User")

	emp := Employee{Name: "John", Age: 30}
	lr.InfofWithProperties("API Called", log.LoggerProperty{Key: "Employee", Value: emp})

	err := generateRandomError()
	if err != nil {
		lr.Error(err.Error(), err)
	}

	randomError()

	time.Sleep(25 * time.Second)
	lr.Info("Exiting")
}

func handlePanic(lr *log.Logger) {
	if err := recover(); err != nil {
		switch v := err.(type) {
		case error:
			lr.Error("Recovered from panic in main()", v)
		case string:
			lr.Error(fmt.Sprintf("Recovered from panic in main(): %s", v), nil)
		default:
			lr.Error("Recovered from panic in main()", fmt.Errorf("%v", err))
		}
	}
}

type Employee struct {
	Name string
	Age  int
}

func generateRandomError() error {
	errors := []error{
		errors.New("database connection failed"),
		errors.New("invalid input"),
		errors.New("network connection failed"),
		errors.New("unknown error"),
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	return errors[rand.Intn(len(errors))]
}

func randomError() {
	currentSecond := time.Now().Second()
	if currentSecond%2 != 0 {
		result, err := divide(10, 0)
		if err != nil {
			panic(err) // Simulating a crash for odd seconds
		}
		fmt.Println("Result:", result)
	}
}

func divide(a, b int) (int, error) {
	if b == 0 {
		return 0, errors.New("division by zero")
	}
	return a / b, nil
}
