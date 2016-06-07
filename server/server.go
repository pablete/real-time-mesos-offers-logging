package server

import (
	"net/http"

	"html/template"

	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	gorWs "github.com/gorilla/websocket"
	"github.com/mesos/mesos-go/mesosproto"
)

var wsUpgrader = gorWs.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var addListenerCh chan chan mesosproto.Offer
var removeListenerCh chan chan mesosproto.Offer
var OfferCh chan mesosproto.Offer //TODO Candidate for singleton

func New(p string, q chan bool, oCh chan mesosproto.Offer) {
	go LaunchDispatcher(oCh)

	r := mux.NewRouter()

	r.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		WebSocketHandler(w, r, q)
	})

	r.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("string"))
	})

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveStatic(p, w, r)
	})

	staticHandler := http.FileServer(http.Dir("server/public/"))
	staticHandler = http.StripPrefix("/public/", staticHandler)
	r.PathPrefix("/public/").Handler(staticHandler)

	log.Infof("Launching server on %s", p)
	log.Fatal(http.ListenAndServe(p, r))
}

func serveStatic(p string, w http.ResponseWriter, r *http.Request) {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	tmpl, err := template.ParseFiles(dir + "/server/templates/index.html")
	if err != nil {
		log.Fatal(err)
	}

	templateData := struct {
		Port string
	}{
		Port: p,
	}

	err = tmpl.Execute(w, templateData)
	if err != nil {
		log.Fatal("Could not launch web server")
	}
}

func WebSocketHandler(w http.ResponseWriter, r *http.Request, q chan bool) {
	log.Info("Client connected")
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("Connection with websocket client not established")
		conn.Close()
		log.Error(err)
		return
	}

	myCh := make(chan mesosproto.Offer)
	addListenerCh <- myCh

	for {
		select {
		case v := <-myCh:
			conn.WriteJSON(v)
		case <-q:
			log.Info("Closing ws connection")
			removeListenerCh <- myCh
			conn.Close()
			return
		}
	}
}

func LaunchDispatcher(oCh chan mesosproto.Offer) {
	log.Info("Launching Dispatcher")
	OfferCh = oCh

	addListenerCh = make(chan chan mesosproto.Offer, 5)
	removeListenerCh = make(chan chan mesosproto.Offer, 5)

	listeners := make([]chan mesosproto.Offer, 0)
	for {
		select {
		case offer := <-OfferCh:
			for _, listener := range listeners {
				listener <- offer
			}
		case listener := <-addListenerCh:
			log.Info("Adding listener")
			listeners = append(listeners, listener)
		case listenerToRemove := <-removeListenerCh:
			log.Info("Removing listener")
			newListeners := make([]chan mesosproto.Offer, 0)
			for _, currentListener := range listeners {
				if currentListener != listenerToRemove {
					newListeners = append(newListeners, currentListener)
				} else {
					close(currentListener)
				}
			}
			listeners = newListeners
		}
	}
}

//
//func home(w http.ResponseWriter, r *http.Request) {
//
//	homeTemplate.Execute(w, "ws://"+r.Host+"/ws")
//}

//var homeTemplate = template.Must(template.New("").Parse(`
//<!DOCTYPE html>
//  <head>
//  <meta charset="utf-8">
//    <script>
//    window.addEventListener("load", function(evt) {
//        var output = document.getElementById("output");
//        var input = document.getElementById("input");
//        var ws;
//        var print = function(message) {
//            var d = document.createElement("div");
//            d.innerHTML = message;
//            output.appendChild(d);
//        };
//            if (ws) {
//                return false;
//            }
//            ws = new WebSocket("{{.}}");
//            ws.onopen = function(evt) {
//                print("OPEN");
//            }
//            ws.onclose = function(evt) {
//                print("CLOSE");
//                ws = null;
//            }
//            ws.onmessage = function(evt) {
//                print("RESPONSE: " + evt.data);
//            }
//            ws.onerror = function(evt) {
//                print("ERROR: " + evt.data);
//            }
//        document.getElementById("close").onclick = function(evt) {
//            if (!ws) {
//                return false;
//            }
//            ws.close();
//            return false;
//        };
//    });
//    </script>
//  </head>
//  <body>
//    <div id="output"></div>
//  </body>
//</html>
//`))
