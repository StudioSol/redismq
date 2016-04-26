package redismq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

// Package provides headers and handling functions around payloads
type Package struct {
	Payload    []byte
	CreatedAt  time.Time
	Queue      *Queue      `json:"-"`
	Consumer   *Consumer   `json:"-"`
	Collection *[]*Package `json:"-"`
	Acked      bool        `json:"-"`
	//TODO add Headers or smth. when needed
	//wellle suggested error headers for failed packages
}

func unmarshalPackage(input string, queue *Queue, consumer *Consumer) (*Package, error) {
	p := &Package{Queue: queue, Consumer: consumer, Acked: false}

	var dec = gob.NewDecoder(bytes.NewBuffer([]byte(input)))

	err := dec.Decode(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (pack *Package) getString() string {
	gob.Register(Package{})
	gob.Register(Queue{})
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(*pack); err != nil {
		panic(err)
	}
	return string(buffer.Bytes())
}

func (pack *Package) index() int {
	if pack.Collection == nil {
		return 0
	}
	var i int
	for i = range *pack.Collection {
		if (*pack.Collection)[i] == pack {
			break
		}
	}
	return i
}

// MultiAck removes all packaes from the fetched array up to and including this package
func (pack *Package) MultiAck() (err error) {
	if pack.Collection == nil {
		return fmt.Errorf("cannot MultiAck single package")
	}
	// TODO write in lua
	for i := 0; i <= pack.index(); i++ {
		var p *Package
		p = (*pack.Collection)[i]
		// if the package has already been acked just skip
		if p.Acked == true {
			continue
		}

		err = pack.Consumer.ackPackage(p)
		if err != nil {
			break
		}
		p.Acked = true
	}
	return
}

// Ack removes the packages from the queue
func (pack *Package) Ack() error {
	if pack.Collection != nil {
		return fmt.Errorf("cannot Ack package in multi package answer")
	}
	err := pack.Consumer.ackPackage(pack)
	return err
}

// Requeue moves a package back to input
func (pack *Package) Requeue() error {
	return pack.reject(true)
}

// Fail moves a package to the failed queue
func (pack *Package) Fail() error {
	return pack.reject(false)
}

func (pack *Package) reject(requeue bool) error {
	if pack.Collection != nil && (*pack.Collection)[pack.index()-1].Acked == false {
		return fmt.Errorf("cannot reject package while unacked package before it")
	}

	if !requeue {
		err := pack.Consumer.failPackage(pack)
		return err
	}
	err := pack.Consumer.requeuePackage(pack)
	return err
}
