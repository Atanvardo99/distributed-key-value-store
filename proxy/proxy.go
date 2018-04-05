package main

import(
	"net/http"
	"encoding/json"
	"time"
	"log"
	"flag"
	"bytes"
	"strconv"
	"math/rand"
	"strings"
)

var StartTime time.Time
var results []string
var serverTable = make(map[int]serverStatus)
const maximum = 1<< 8
var serverNum = 0
type Reqs struct{

	Reqtype string
	M map[string]string
	TimeStamp time.Time
	Identify string
}

type heartBeat struct{

	Status string
	Uptime string
}
type heartBeatReq struct{

	ReqType string

}
type serverStatus struct{
	addr string
	// 0 = unboot
	// 1 = booting, initialization, sync data from other server, not ready

	// 2 = normal, regularly handling get/post request
	// 3 = full, oom or other condition, regularly handling select & delete & update, but not available for insert,
	// 		insert may re-assign to other servers
	// 4 = shutting down, handling all requests, but may need to look up other servers,
	// 		sync data to other server, if logout, proxy acquires data from other servers

	// 9 = fault, not available at all (since its a simple scenario, server may never come up with a fault condition due to precondition,
	// 		but this condition should be considered)

	status int

}






func addServer(s string) int{
	i := 0
	if len(serverTable) == 0{
		serverTable[i] = serverStatus{s, 1}
		return 0
	}else {

		for k, v := range serverTable{
			if v.status == 0{
				serverTable[k] = serverStatus{s, 1}
				return k
			}
		}
		rand.Seed(95279527)
		for true{
			i = rand.Intn(maximum)
			//var ok bool
			if _, ok := serverTable[i]; !ok{
				serverTable[i] = serverStatus{s, 1}
				return i
			}
		}
	}
	return 0
}

func assign(i int) int{

	if _, ok := serverTable[i]; ok && serverTable[i].status !=0{
		return i
	}
	for ; i  >= 0; i--{
		if _, ok := serverTable[i]; ok{
			if serverTable[i].status ==0{
				return assign(i - 1)
			}
			return i
		}
	}
	for k := maximum - 1; k > i; k--{
		if _, ok := serverTable[k]; ok{
			if serverTable[k].status ==0{
				return assign(k - 1)
			}
			return k
		}
	}
	return 0
}

func updateServerStatus(s string, addr string, id string) int{

	log.Println("update server: ", s, addr, id)
	var statusNum int
	var serverNum int
	switch s {
	case "onboot":{
		statusNum = 1
	}
	case "normal": {
		statusNum = 2
	}
	case "full": {
		statusNum = 3
	}
	case "onshut": {
		statusNum = 4
	}
	case "fault": {
		statusNum = 9
	}
	default:
		statusNum = 0
	}

	if statusNum > 1 && statusNum <= 4{
		serverNum, err := strconv.Atoi(id)
		if nil != err{

			log.Println("unconvertable id: ", id)
		}
		if serverTable[serverNum].status != statusNum{
			serverTable[serverNum] = serverStatus{serverTable[serverNum].addr, statusNum}
		}

	}
	if statusNum == 1{

		return addServer(addr)
	}
	log.Println(serverNum)
	return serverNum

}
func getAvailableServer(s string) map[int]serverStatus{
	var status map[int]serverStatus = make(map[int]serverStatus)
	switch s {
	case "add", "addmap":{

		for k, v := range serverTable{
			if v.status == 2 || v.status == 0{
				status[k] = v
			}

		}
	}
	case "choose", "remove", "modify":{

		for k, v := range serverTable{
			if v.status == 2 || v.status == 3 || v.status == 4 || v.status == 0{
				status[k] = v
			}
		}
	}
	}

	return status
}

func removeServer(s string){

	num, err := strconv.Atoi(s)
	if nil != err{
		log.Println("unconvertable id: ", s)

	}
	status := serverStatus{serverTable[num].addr, 0}

	serverTable[num] = status
}

func maxNum(m map[int]serverStatus) int{
	var i = 0
	for k, _ := range m{

		if k > i{
			i = k
		}
	}
	return i
}
func minNum(m map[int]serverStatus) int{
	var i = 1<<31
	for k, _ := range m{

		if k < i{
			i = k
		}
	}
	return i
}
func availableNext(m map[int]serverStatus, i int) int{

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
func hashes(s string) int64{
	var key = []byte(s)
	var hash = int64(9527)
	for c := range key{

		hash = (hash * 33) + hash + (int64(key[c]))

	}
	return hash
}

func getHeartBeat(address string, c chan serverStatus) (s serverStatus, err error ) {


	jsonBody := heartBeatReq{"touch"}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("GET", address, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("try to touch ", address,  jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	var resps serverStatus
	json.NewDecoder(resp.Body).Decode(&resps)
	log.Println("get response:", resps)

	// TODO: parse responses
	var status = resps
	log.Println(status)
		c <- status
	return s, nil
}

func handler(rw http.ResponseWriter, r *http.Request) {
	uptime := time.Since(StartTime).String()
	_, err := json.Marshal(heartBeat{"running", uptime})
	if err != nil {
		log.Fatalf("Failed to write heartbeat message. Reason: %s", err.Error())
	}
}

func touch(){

	for true{
		//var v serverStatus
		time.Sleep(30 * time.Second)
		if len(serverTable) > 0 {
			c := make(chan serverStatus)
			for _, v := range serverTable {
				if v.status != 0 && v.status != 1  {
					go getHeartBeat(v.addr, c)
				}
			}
			status := <-c
			log.Println("get heart beat of ", status, " \nall server status", serverTable)

		}


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
		reqType := req.Reqtype
		log.Println("msg: ", req)

		switch reqType {

		// from client
		case "add", "addmap", "remove", "modify", "choose": {
			var available = getAvailableServer(reqType)
			var reassign  = make(map[serverStatus] map[string]string)
			log.Println(serverTable)
			for k, v := range req.M{

				// server status
				var key = assign(int(hashes(k)) % int(maximum))
				log.Println("-----------", k, v,int(hashes(k)) % int(maximum), key)
				if _, ok := available[key]; ok && available[key].status != 0{

					serverMap := reassign[available[key]]
					if nil == serverMap {
						serverMap = make(map[string] string)
					}
					serverMap[k] = v
					reassign[available[key]] = serverMap

				}else {

					// hash one more time, reassign to available server, this could be done several times, but for simple scenario
					var newKey = assign(int(int(hashes(string(int64(key)))) % int(maximum)))
					if _, ok := available[newKey];  ok && available[newKey].status != 0 {
						serverMap := reassign[available[newKey]]
						if nil == serverMap {
							serverMap = make(map[string] string)
						}
						serverMap[k] = v
						reassign[available[newKey]] = serverMap

					}else {
						var next = availableNext(available, newKey)
						serverMap := reassign[available[next]]
						if nil == serverMap {
							serverMap = make(map[string] string)
						}
						serverMap[k] = v
						reassign[available[next]] = serverMap
					}
				}

			}

			// TODO: post requests to servers according to reassignment
			c := make(chan Reqs)
			for server, data := range reassign{
				go doPost( "POST",reqType, server, data, req.Identify, w, c)
			}
			respMsg := <- c
			json.NewEncoder(w).Encode(respMsg)
			log.Println("response: ", respMsg)
		}

		// servers status
		case "onboot", "onshut", "normal", "full":{
			log.Println("req: ", reqType)
			var addr = req.M["addr"]
			var serverNum = updateServerStatus(reqType, addr, req.Identify)
			log.Println("identified: ", strconv.Itoa(serverNum))

			var respMsg = Reqs{"identified", nil, time.Now(), strconv.Itoa(serverNum)}

			if "onboot" == reqType{
				var availableServer = getAvailableServer("choose")
				var serverMap = make(map[string] serverStatus)
				serverMap["max"] = serverStatus{"",len(serverTable)}
				for k, v := range availableServer{
					serverMap[strconv.Itoa(k)] = v
				}
				log.Println("identified: ", strconv.Itoa(serverNum))

				reqMap := make(map[string]string)
				for k, v := range serverMap{
					reqMap[k] = v.addr+ strconv.Itoa(v.status)

				}
				respMsg.M = reqMap
			}else if "onshut" == reqType{
				var availableServer = getAvailableServer("add")
				var serverMap = make(map[string] serverStatus)
				for k, v := range availableServer{
					serverMap[strconv.Itoa(k)] = v
				}
				reqMap := make(map[string]string)
				for k, v := range serverMap{
					reqMap[k] = v.addr+ strconv.Itoa(v.status)
				}
				respMsg.M = reqMap
				log.Println("map: ", respMsg.M)

			}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(serverTable)
		}

		case "shutdown": {

			removeServer(req.Identify)
			log.Println(serverTable)
			var respMsg = Reqs{"bye", nil, time.Now(), ""}
			json.NewEncoder(w).Encode(respMsg)

		}
		case "serverlist" :{

			var availableServer = getAvailableServer("choose")
			var serverMap = make(map[string] serverStatus)
			for k, v := range availableServer{
				serverMap[strconv.Itoa(k)] = v
			}
			reqMap := make(map[string]string)
			for k, v := range serverMap{
				reqMap[k] = strings.Join([]string{v.addr, strconv.Itoa(v.status)},"")

			}
			var respMsg = Reqs{"serverlist", reqMap, time.Now(), ""}
			json.NewEncoder(w).Encode(respMsg)

			log.Println(serverMap)


			log.Println(serverTable)
		}
		}
	}
}

func doPost(reqtype string, request string, server serverStatus, data map[string] string, id string,w http.ResponseWriter, c chan Reqs ){
	log.Println("assignment to ", server, data)
	jsonBody := Reqs{request,data, time.Now(),  id}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	assign, err := http.NewRequest(reqtype, server.addr, encode)
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


	var respMsg = Reqs{resps.Reqtype, resps.M, time.Now(), id}
	json.NewEncoder(w).Encode(respMsg)
	log.Println("response: ", respMsg)
	c <- resps
}

func init() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
}

func main() {

	results = append(results, time.Now().Format(time.RFC3339))

	mux := http.NewServeMux()
	mux.HandleFunc("/", PostHandler)
	go touch()
	log.Fatal(http.ListenAndServe(":9000", mux))


}