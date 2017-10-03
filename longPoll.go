package apiGatewayConfDeploy

import "time"

// The channel sent to addSubscriber should be buffered channel
func distributeEvents(deliverChan <- chan interface{}, addSubscriber chan chan interface{}) {
	subscribers := make([]chan interface{}, 0)
	for {
		select {
		case element, ok := <-deliverChan:
			if !ok {
				return
			}
			subs := subscribers
			subscribers = make([]chan interface{}, 0)
				for _, subscriber := range subs {
					go func(sub chan interface{}) {
						log.Debugf("delivering to: %v", sub)
						sub <- element
					}(subscriber)
				}
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