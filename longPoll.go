// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apiGatewayConfDeploy

import "time"

// distributeEvents() receives elements from deliverChan, and send them to subscribers
// Sending a `chan interface{}` to addSubscriber adds a new subscriber.
// It closes the subscriber channel after sending the element.
// Any subscriber sent to `addSubscriber` should be buffered chan.
func distributeEvents(deliverChan <-chan interface{}, addSubscriber chan chan interface{}) {
	subscribers := make([]chan interface{}, 0)
	for {
		select {
		case element, ok := <-deliverChan:
			if !ok {
				return
			}
			for _, subscriber := range subscribers {
				go func(sub chan interface{}) {
					log.Debugf("delivering to: %v", sub)
					sub <- element
					close(sub)
				}(subscriber)
			}
			subscribers = make([]chan interface{}, 0)
		case sub, ok := <-addSubscriber:
			if !ok {
				return
			}
			log.Debugf("Add subscriber: %v", sub)
			subscribers = append(subscribers, sub)
		}
	}
}

func debounce(in chan interface{}, out chan interface{}, window time.Duration) {
	send := func(toSend interface{}) {
		if toSend != nil {
			out <- toSend
		}
	}
	var toSend interface{} = nil
	for {
		select {
		case incoming, ok := <-in:
			if ok {
				log.Debugf("debouncing %v", incoming)
				toSend = incoming
			} else {
				send(toSend)
				log.Debugf("closing debouncer")
				close(out)
				return
			}
		case <-time.After(window):
			send(toSend)
			toSend = nil
		}
	}
}
