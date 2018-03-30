package main


import(
	//"fmt"
	//"io/ioutil"
	"net/http"
	"encoding/json"
	"time"
	"log"
	"flag"
	"bytes"
	"fmt"
	//"strconv"
	"os"
	"os/signal"
	"syscall"
	"strconv"
)
const port string = ":9010"
const proxy string = "http://localhost:9000"

const local = "http://localhost" + port

var results []string
var serverTable map[int] dataFlow
var serverId string = ""
// simplified maximum storage upper bound
const maxStorage int = 100


// storage, outside key is client id, with value of its key-value
// selfstore contains keys of corresponding hash value, while helpstore constians data from other abnormal server
var selfstore = make(map[string] map[string]string)
var helpstore = make(map[string] map[string]string)

var totalNum int
var status string

type Reqs struct{

	Reqtype string
	M map[string]string
	TimeStamp time.Time
	Identify string

}
type Acquire struct {

	Reqtype string
	M map[string] map[string]string
	TimeStamp time.Time
	Identify string
}
//type ServerReq struct{
//	Reqtype string
//	Addr string
//	TimeStamp time.Time
//	Identify string
//}

type heartBeat struct{

	Status string
	Uptime string
}
type heartBeatReq struct{

	ReqType string

}
type dataFlow struct{

	addr string

	// 0: no data transaction
	// 1: get data from server
	// 2: send data to server
	flow int
}

func availableNext(m map[int]string, i int) int{

	//var next = 0
	var temp = 1 << 31
	for k, _ := range m{

		if k - i < temp && k - i > 0{
			temp = k - i
		}

	}
	if temp < 1<< 31{
		return i + temp

	}else{
		return minNum(m)
	}
}

func minNum(m map[int]string) int{
	var i = 1<<31
	for k, _ := range m{

		if k < i{
			i = k
		}
	}
	return i
}

func hash(key []byte) int64{
	var hash = int64(9527)
	for c := range key{

		hash = (hash * 33) + hash + (int64(key[c]))

	}
	return hash
}
func hashes(s string) int64{
	var key = []byte(s)
	var hash = int64(9527)
	for c := range key{

		hash = (hash * 33) + hash + (int64(key[c]))

	}
	return hash
}

func afterboot(){
	log.Println("post: ", serverId)
	status = "normal"
	jsonBody := Reqs{"normal",map[string]string{"addr": local }, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", proxy, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("server onboot", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("response:", resp)

}
func acquire(addr string,  c chan string ){
	log.Println("post: ", serverId)

	jsonBody := Reqs{"acquire",nil, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", addr, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("server acquire", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("response:", resp)

	var req Acquire
	if resp.Body == nil {
		log.Println("error")
		//http.Error(w, "Please send a request body", 400)
		return
	}
	resperr := json.NewDecoder(resp.Body).Decode(&req)
	if resperr != nil {
		//http.Error(w, resperr.Error(), 400)
		return
	}
	//id := req.Identify
	// add
	for id, data := range req.M{

		for k, v := range data {
			if store, ok := selfstore[id]; ok {
				store[k] = v
			} else {
				selfstore[id] = make(map[string]string)
				selfstore[id][k] = v
			}
		}
	}

	c <- addr

}

func choose(id string, key string) (s string, b bool){

	//var data map[string]string
	if data, b := selfstore[id]; b{
		//var v string
		if v, b := data[key]; b{
			return v, true

		}else {
			return "", false
		}

	}else {
		if data, b:= helpstore[id]; b{

			//var v string
			if v, b := data[key]; b{
				return v, true

			}else {
				return "", false
			}

		}else {
			return "", false

		}

	}


}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	//fmt.Println("msg", r)
	if r.Method == "POST" {
		fmt.Println("post")
		var req Reqs
		if r.Body == nil {
			fmt.Println("error")
			http.Error(w, "Please send a request body", 400)
			return
		}
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		id := req.Identify
		switch req.Reqtype {

		// request from client
		case "add", "addmap":{
			for k, v := range req.M{

				if store, ok := selfstore[id]; ok{
					store[k] = v
				}else {
					selfstore[id] = make(map[string]string)
					selfstore[id][k] = v
				}
			}
			// TODO: response
			var respMsg = Reqs{"succeed", nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(respMsg)
		}

		case "remove":{

			for k, _ := range req.M{

				if store, ok := selfstore[id]; ok{

					delete(store, k)
					if len(store) == 0{
						delete(selfstore, id)
					}
				}

			}
			// TODO: response
			var respMsg = Reqs{"succeed", nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(respMsg)
		}
		//case "modify": {
		//
		//
		//}
		case "choose":{
			for k, _ := range req.M{
				var s string
				var ok bool
				s, ok = choose(id, k)
				log.Println(s, ok)
				// TODO: response
				m := make(map[string]string)
				m[k] = s
				var respMsg = Reqs{"succeed", m, time.Now(), serverId}
				json.NewEncoder(w).Encode(respMsg)
				log.Println(respMsg)
			}

		}

		// request from other server
		case "helpadd":{


		}
		case "release":{


		}
		}
		log.Println("selfstore: ", selfstore)
		log.Println("helpstore: ", helpstore)
	} else if r.Method == "GET" {
		fmt.Println("get")
		var u Reqs
		if r.Body == nil {
			fmt.Println("error")
			http.Error(w, "Please send a request body", 400)
			return
		}
		//fmt.Println(r.Body)
		err := json.NewDecoder(r.Body).Decode(&u)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		//fmt.Println(u)
		switch u.Reqtype{

		// heart beat of proxy
		case "touch":{

			var respMsg = Reqs{status, nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)

		}

		}
	} else{

		fmt.Fprint(w, r.Method)
	}
}
func init() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	status = "onboot"
	jsonBody := Reqs{"onboot",map[string]string{"addr": local }, time.Now(),  "", }
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", proxy, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("server onboot", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	// TODO: parse responses
	var id Reqs
	json.NewDecoder(resp.Body).Decode(&id)
	serverId = id.Identify
	serverList := id.M

	log.Println("identified: ", serverId)


	c := make(chan string)

	for _, v := range serverList {
		go acquire(v, c)
	}
	r := <- c
	log.Println("acquire from:", r)



	afterboot()
	log.Println("response:", resp)
}
func reassign(addr string,  c chan string ){
	log.Println("post: ", serverId)

	jsonBody := Reqs{"helpadd",nil, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", addr, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("server helpadd", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("response:", resp)

	var req Reqs
	if resp.Body == nil {
		log.Println("error")
		//http.Error(w, "Please send a request body", 400)
		return
	}
	resperr := json.NewDecoder(resp.Body).Decode(&req)
	if resperr != nil {
		//http.Error(w, resperr.Error(), 400)
		return
	}
	//id := req.Identify
	// add
	//for id, data := range req.M{
	//
	//	for k, v := range data {
	//		if store, ok := selfstore[id]; ok {
	//			store[k] = v
	//		} else {
	//			selfstore[id] = make(map[string]string)
	//			selfstore[id][k] = v
	//		}
	//	}
	//}

	c <- addr

}

func shutdown(b chan bool){
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs
	fmt.Println()
	fmt.Println(sig)

	// get available serverlist
	jsonBody := Reqs{"onshut",nil, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", proxy, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("shutting down ", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	var resps Reqs
	json.NewDecoder(resp.Body).Decode(&resps)
	log.Println("get response:", resps)
	// TODO: parse responses
	serverList := resps.M
	//log.Println(s)
	//return s, returnMap,nil


	var reassign  = make(map[string] map[string]string)
	var i = 0
			for k, _ := range serverTable{

			if k > i{
				i = k
				}
			}
	addr := make(chan string)
		for user, data := range selfstore{
			for k, v := range data{
				intk,_ := strconv.Atoi(k)
				newKey:= strconv.Itoa(int(hashes(string(int64(intk) * 9527 << 5)) % int64(i)))

				if _, ok := serverList[newKey]; ok {
					//reassign[available[key]] = m
					serverMap := reassign[serverList[newKey]]
					if nil == serverMap {
						serverMap = make(map[string] string)
					}
					serverMap[k] = v
					reassign[serverList[newKey]] = serverMap

				}else {
					var next = availableNext(serverList, newKey)
					serverMap := reassign[serverList[next]]
					if nil == serverMap {
						serverMap = make(map[string] string)
					}
					serverMap[k] = v
					reassign[serverList[next]] = serverMap
				go

			}

		}
		//for
		//var newKey = int(hashes(string(int64(key) * 9527 << 5)) % int64(max))
		//if _, ok := available[newKey]; ok {
		//	//reassign[available[key]] = m
		//	serverMap := reassign[available[newKey]]
		//	if nil == serverMap {
		//		serverMap = make(map[string] string)
		//	}
		//	serverMap[k] = v
		//	reassign[available[newKey]] = serverMap
		//
		//}else {
		//	var next = availableNext(available, newKey)
		//	serverMap := reassign[available[next]]
		//	if nil == serverMap {
		//		serverMap = make(map[string] string)
		//	}
		//	serverMap[k] = v
		//	reassign[available[next]] = serverMap
		//
		//jsonBody := Reqs{"helpadd",, time.Now(),  id}
		//encode := new(bytes.Buffer)
		//json.NewEncoder(encode).Encode(jsonBody)
		//client := http.Client{}
		////url := url.URL{Host:"localhost:9001"}
		//request, err := http.NewRequest("POST", proxy, encode)
		//if err != nil {
		//	log.Fatalln(err)
		//}
		//log.Println("post: add ", jsonBody)
		//resp, err := client.Do(request)
		//if err != nil {
		//	log.Fatalln(err)
		//}
		//var resps Reqs
		//json.NewDecoder(resp.Body).Decode(&resps)
		//log.Println("get response:", resps)
		//// TODO: parse responses
		//s, returnMap = parseReq(resps)
		//log.Println(s)

		b <- true


}
func main() {
	done := make(chan bool, 1)

	results = append(results, time.Now().Format(time.RFC3339))
	mux := http.NewServeMux()
	mux.HandleFunc("/", PostHandler)

	log.Fatal(http.ListenAndServe(port, mux))

	go shutdown(done)
	<-done
	fmt.Println("exiting")
	//mux := http.NewServeMux()
	//mux.HandleFunc("/", GetHandler)
	//mux.HandleFunc("/post", PostHandler)
	//http.Post("localhost:9000", "123", nil)
	//log.Printf("listening on port %s", *flagPort)
	//log.Fatal(http.ListenAndServe(":" + *flagPort, mux))
}