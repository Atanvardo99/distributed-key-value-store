package main


import(
	"net/http"
	"encoding/json"
	"time"
	"log"
	"flag"
	"bytes"
	"os"
	"os/signal"
	"syscall"
	"strconv"
)
const port string = ":9010"
const proxy string = "http://localhost:9000"
const maximum = 1<< 8

const local = "http://localhost" + port

var results []string
var serverId string = ""
var serverTable = make(map[string]serverStatus)
var serverMap = make(map[string]string)
var max = 0
const maxStorage int = 100


var store = make(map[string]string)
var totalNum int
var status string

type Reqs struct{

	Reqtype string
	M map[string]string
	TimeStamp time.Time
	Identify string

}
type serverStatus struct{
	addr string
	status int

}

type dataFlow struct{

	addr string
	// 0: no data transaction
	// 1: get data from server
	// 2: send data to server
	flow int
}

func availableNext(m map[string]serverStatus, si string) int{

	var temp = 1 << 31
	i,_ := strconv.Atoi(si)
	for k, v := range m{
		ki,_ := strconv.Atoi(k)
		if ki - i < temp && ki - i > 0 && v.status != 0{
			temp = ki - i
		}

	}
	if temp < 1<< 31{
		return i + temp

	}else{
		return minNum(m)
	}
}
func assign(i int) int{

	if _, ok := serverTable[strconv.Itoa(i)]; ok &&serverTable[strconv.Itoa(i)].status !=0{
		return i
	}
	for ; i  >= 0; i--{
		if _, ok := serverTable[strconv.Itoa(i)]; ok{
			if serverTable[strconv.Itoa(i)].status  ==0{
				return assign(i - 1)
			}
			return i
		}
	}
	for k := maximum - 1; k > i; k--{
		if _, ok := serverTable[strconv.Itoa(k)]; ok{
			if serverTable[strconv.Itoa(k)].status  ==0{
				return assign(k - 1)
			}
			return k
		}
	}
	return 0
}

func minNum(m map[string]serverStatus) int{
	var i = 1<<31
	for k, v := range m{
		ki,_ := strconv.Atoi(k)
		if ki < i && v.status != 0{
			i = ki
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
	var maxmap = make(map[string]string)
	maxmap["max"] = strconv.Itoa(max)
	for k, v := range serverTable{
		serverMap[k] = v.addr+ strconv.Itoa(v.status)
	}
	serverMap[serverId] = addr + "2"
	jsonBody := Reqs{"acquire",serverMap, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	request, err := http.NewRequest("POST", addr, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(serverTable)
	log.Println("server acquire", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("response:", resp)

	var req Reqs
	if resp.Body == nil {
		log.Println("error")
		return
	}
	resperr := json.NewDecoder(resp.Body).Decode(&req)
	if resperr != nil {
		return
	}
	for k, v := range req.M{
		store[k] = v
	}
	c <- addr

}

func choose(key string) (s string, b bool){

	if v, b := store[key]; b{
		return v, true

	}else {
		return "", false
	}
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var req Reqs
		if r.Body == nil {
			http.Error(w, "Please send a request body", 400)
			return
		}
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		switch req.Reqtype {

		case "add", "addmap":{
			log.Println("get add ", req.M)
			for k, v := range req.M{
				store[k] = v
			}
			// TODO: response
			var respMsg = Reqs{"succeed", nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(respMsg)
		}

		case "remove":{

			for k, _ := range req.M{

				if _, ok := store[k]; ok{
					delete(store, k)
				}

			}
			// TODO: response
			var respMsg = Reqs{"succeed", nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(respMsg)
		}
		case "choose":{
			for k, _ := range req.M{
				var s string
				var ok bool
				s, ok = choose(k)
				log.Println(s, ok)
				// TODO: response
				m := make(map[string]string)
				m[k] = s
				var respMsg = Reqs{"succeed", m, time.Now(), serverId}
				json.NewEncoder(w).Encode(respMsg)
				log.Println(respMsg)
			}

		}
		case "acquire":{
			var reassign = make(map [string]string)
			id, _ := strconv.Atoi(req.Identify)
			log.Println("acquire get")
			serverMap := req.M
			log.Println(serverMap)
			for k, v := range serverMap{
				if k == "max"{

				}else{
					log.Println("k ", k, ", v ", v)
					addr := v[0: len(v) -1]
					s, _:= strconv.Atoi(v[len(v) - 1: len(v)])
					log.Println("addr ", addr, ", s ", s)

					key, _ := strconv.Atoi(k)
					serverTable[strconv.Itoa(key)] = serverStatus{addr, s}
				}
			}
			for k, v := range store{
				intk := assign(int(hashes(k)) % int(maximum))
				log.Println(serverTable)

				log.Println("id: ", id , ", intk: ", intk, "    ", int(hashes(k)) % int(maximum) )

				if intk == id{
					reassign[k] = v
				}

			}
			log.Println(reassign)
			if len(reassign) >0 {
				var respMsg = Reqs{"successed", reassign, time.Now(), serverId}
				json.NewEncoder(w).Encode(respMsg)

				for k,_ := range reassign{
					delete(store, k)
				}
			}else {
				var respMsg = Reqs{"successed", nil, time.Now(), serverId}
				json.NewEncoder(w).Encode(respMsg)
			}

		}
		}
		log.Println("store: ", store)
	} else if r.Method == "GET" {
		var u Reqs
		if r.Body == nil {
			http.Error(w, "Please send a request body", 400)
			return
		}
		err := json.NewDecoder(r.Body).Decode(&u)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		switch u.Reqtype{

		case "touch":{
			log.Println("get in touch with proxy")
			var respMsg = Reqs{status, nil, time.Now(), serverId}
			json.NewEncoder(w).Encode(respMsg)
			log.Println("data", store)
		}

		}
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

	serverMap := id.M
	log.Println(serverMap)
	for k, v := range serverMap{
		if k == "max"{

		}else{
			log.Println("k ", k, ", v ", v)
			addr := v[0: len(v) -1]
			s, _:= strconv.Atoi(v[len(v) - 1: len(v)])
			log.Println("addr ", addr, ", s ", s)

			key, _ := strconv.Atoi(k)
			serverTable[strconv.Itoa(key)] = serverStatus{addr, s}
		}
	}
	max, _ = strconv.Atoi(serverMap["max"])
	log.Println("identified: ", serverId)
	log.Println("server table: ",serverTable)
}

func doPost(reqtype string, request string, addr string, data map[string] string, id string, c chan Reqs){
	log.Println("assignment to ", addr, data)
	jsonBody := Reqs{request,data, time.Now(),  id}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	assign, err := http.NewRequest(reqtype, addr, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(reqtype, request, jsonBody)
	resp, err := client.Do(assign)
	if err != nil {
		log.Fatalln(err)
	}

	// TODO: parse responses
	var resps Reqs
	json.NewDecoder(resp.Body).Decode(&resps)
	log.Println(resps)

	c <- resps
}


func reassign(addr string,  c chan string ){
	log.Println("post: ", serverId)

	jsonBody := Reqs{"addmap",nil, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	request, err := http.NewRequest("POST", addr, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("server help add", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("response:", resp)

	var req Reqs
	if resp.Body == nil {
		log.Println("error")
		return
	}
	resperr := json.NewDecoder(resp.Body).Decode(&req)
	if resperr != nil {
		return
	}
	c <- addr

}

func shutdown(b chan bool){
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs
	log.Println(sig)

	jsonBody := Reqs{"onshut",nil, time.Now(),  serverId}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
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

	for k, v := range serverList{
		s, _ := strconv.Atoi(v[len(v )- 1: len(v)])
		serverTable[k] = serverStatus{v[0:len(v) - 1], s}
	}
	var reassign  = make(map[string] map[string]string)
	var i = 0
	for ki, _ := range serverTable{
		k,_ := strconv.Atoi(ki)
		if k > i{
			i = k
		}
	}
	log.Print("server Table ", serverTable)
	delete(serverTable, serverId)
	for k, v := range store {
		intk, _ := strconv.Atoi(k)
		var newKey = strconv.Itoa(assign(int(hashes(string(int64(intk) * 9527 << 5)) % int64(maximum))))
		log.Println(newKey)
		if _, ok := serverTable[newKey]; ok {
			serverMap := reassign[serverTable[newKey].addr]
			if nil == serverMap {
				serverMap = make(map[string]string)
			}
			serverMap[k] = v
			reassign[serverTable[newKey].addr] = serverMap

		} else {
			var next = strconv.Itoa(availableNext(serverTable, newKey))
			serverMap := reassign[serverTable[newKey].addr]
			if nil == serverMap {
				serverMap = make(map[string]string)
			}
			serverMap[k] = v
			reassign[serverTable[next].addr] = serverMap

		}

	}
	log.Println("servers help add ")

	var msg Reqs
	c := make(chan Reqs)
	if len(reassign) > 0 {
		for s, m := range reassign {
			go doPost("POST", "addmap", s, m, serverId, c)
		}
		log.Println("servers help add end")
		msg = <-c

	}else {
		log.Println("nothing to add")

	}



	end := make(chan Reqs)
	go doPost("POST" , "shutdown" , proxy , nil, serverId, end)
	msg = <- end
	log.Println(msg)
	b <- true



}

func acquireData(){

	 serverList := make(map [string]string)
	 for k, v:= range serverTable{

	 	if v.status != 0{
	 		serverList[k] = v.addr
		}
	 }
	if len(serverList) > 0 {
		c := make(chan string)
		for _, v := range serverTable {
			if v.status != 0{
				go acquire(v.addr, c)

			}
		}
		r := <-c
		log.Println("acquire from:", r)

	}else {
		log.Println("clean server")
	}
	afterboot()
}

func startHttpServer() *http.Server {
	srv := &http.Server{Addr: port}
	http.HandleFunc("/", PostHandler)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Httpserver: ListenAndServe() error: %s", err)
		}
	}()

	return srv
}


func main() {


	done := make(chan bool, 1)

	results = append(results, time.Now().Format(time.RFC3339))
	go acquireData()
	go shutdown(done)
	srv := startHttpServer()

	<-done
	if err := srv.Shutdown(nil); err != nil {
		panic(err)
	}

	log.Println("end shutdown")

	log.Println("exiting")

}