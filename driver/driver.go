package driver

import "fmt"

type Driver interface {
	Open(dir string, opts *Options) (DB, error)
}

var drivers = map[string]Driver{}

func Register(name string, driver Driver) {
	if _, ok := drivers[name]; ok {
		panic(fmt.Errorf("duplicated driver registration: %s", name))
	}
	drivers[name] = driver
}

func Open(driverName, dbName string, opts *Options) (DB, error) {
	driver := drivers[driverName]
	if driver == nil {
		return nil, fmt.Errorf("no driver for: %s", driverName)
	}
	return driver.Open(dbName, opts)
}
