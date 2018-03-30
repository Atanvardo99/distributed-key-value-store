package main

import(
	"fmt"
	"net/http"
	"encoding/json"
	"time"
	"log"
	"flag"
	"bytes"
	"strconv"
)

var StartTime time.Time
var results []string
var serverTable = make(map[int]serverStatus)

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
	if len(serverTable) == maxNum(serverTable) + 1{
		i = len(serverTable)
	}else {
		//var ok bool

		for i < maxNum(serverTable) {

			if _, ok := serverTable[i]; ok{
				continue
			}else {
				break
			}
		}
	}

	serverTable[i] = serverStatus{s, 1}
	log.Println("add server: ", s, i)
	return i
}

func updateServerStatus(s string, addr string, id string) int{


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

	if statusNum > 1 && statusNum < 4{
		serverNum, err := strconv.Atoi(id)
		if nil != err{

			fmt.Println("unconvertable id: ", id)
		}
		if serverTable[serverNum].status != statusNum{
			serverTable[serverNum] = serverStatus{serverTable[serverNum].addr, statusNum}
		}

	}
	if statusNum == 1{

		return addServer(addr)
	}
	fmt.Println(serverNum)
	return serverNum

}
func getAvailableServer(s string) map[int]serverStatus{
	var status map[int]serverStatus = make(map[int]serverStatus)
	switch s {
	case "add", "addmap":{

		for k, v := range serverTable{
			if v.status == 2{
				status[k] = v
			}

		}
	}
	case "choose", "remove", "modify":{

		for k, v := range serverTable{
			if v.status == 2 || v.status == 3 || v.status == 4{
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
		fmt.Println("unconvertable id: ", s)

	}
	delete(serverTable, num)
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
	//var msg []byte
	uptime := time.Since(StartTime).String()
	_, err := json.Marshal(heartBeat{"running", uptime})
	if err != nil {
		log.Fatalf("Failed to write heartbeat message. Reason: %s", err.Error())
	}
}

//func RunHeartbeatService(address string) {
//	http.HandleFunc("/heartbeat", handler)
//	log.Println(http.ListenAndServe(address, nil))
//}
func touch(){

	for true{
		//var v serverStatus
		time.Sleep(5 * time.Second)
		if len(serverTable) > 0 {
			c := make(chan serverStatus)
			for _, v := range serverTable {
				go getHeartBeat(v.addr, c)

			}
			status := <-c
			log.Println("get heart beat of ", status, " \nall server status", serverTable)

		}


	}

}

// GetHandler handles the index route
//func GetHandler(w http.ResponseWriter, r *http.Request) {
//	jsonBody, err := json.Marshal(results)
//	if err != nil {
//		http.Error(w, "Error converting results to json",
//			http.StatusInternalServerError)
//	}
//	w.Write(jsonBody)
//}

// Handler converts request body to string
func PostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		fmt.Println("post")
		var req Reqs
		if r.Body == nil {
			fmt.Println("error")
			http.Error(w, "Please send a request body", 400)
			return
		}
		fmt.Println(r.Body)
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		//fmt.Println(req)
		reqType := req.Reqtype
		log.Println("msg: ", req)

		switch reqType {

		// from client
		case "add", "addmap", "remove", "modify", "choose": {
			var available = getAvailableServer(reqType)
			fmt.Println("available server: ", available)
			var max = maxNum(available) + 1

			// assignment of servers
			var reassign  = make(map[serverStatus] map[string]string)
			for k, v := range req.M{

				// server status
				var key = int(hashes(k) % int64(max))
				if _, ok := available[key]; ok {
					//reassign[available[key]] = m
					//var serverMap = make(map[string]string)

					serverMap := reassign[available[key]]
					if nil == serverMap {
						serverMap = make(map[string] string)
					}
					serverMap[k] = v
					reassign[available[key]] = serverMap

				}else {

					// hash one more time, reassign to available server, this could be done several times, but for simple scenario
					var newKey = int(hashes(string(int64(key) * 9527 << 5)) % int64(max))
					if _, ok := available[newKey]; ok {
						//reassign[available[key]] = m
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
			//var respMsg = Reqs{"test", nil, time.Now(), ""}
			json.NewEncoder(w).Encode(respMsg)
			log.Println("response: ", respMsg)
		}

		// servers status
		case "onboot", "onshut", "normal", "full":{
			var addr = req.M["addr"]
			var serverNum = updateServerStatus(reqType, addr, req.Identify)
			var respMsg = Reqs{"identified", nil, time.Now(), strconv.Itoa(serverNum)}

			if "onboot" == reqType{
				var availableServer = getAvailableServer("choose")
				var serverMap = make(map[string] string)
				for k, v := range availableServer{
					serverMap[strconv.Itoa(k)] = v.addr
				}
				log.Println("identified: ", strconv.Itoa(serverNum))
				respMsg.M = serverMap
			}
			json.NewEncoder(w).Encode(respMsg)
			log.Println(serverTable)
		}

		case "shutdown": {

			removeServer(req.Identify)
			log.Println(serverTable)

		}
		case "serverlist" :{

			var availableServer = getAvailableServer("choose")
			var serverMap = make(map[string] string)
			for k, v := range availableServer{
				serverMap[strconv.Itoa(k)] = v.addr
			}
			var respMsg = Reqs{"serverlist", serverMap, time.Now(), ""}
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
	//url := url.URL{Host:"localhost:9001"}
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
	//results = append(results, time.Now().Format(time.RFC3339))
	//
	//mux := http.NewServeMux()
	//mux.HandleFunc("/", GetHandler)
	//mux.HandleFunc("/post", PostHandler)
	//http.Post("localhost:9000", "123", nil)
	//log.Printf("listening on port %s", *flagPort)
	//log.Fatal(http.ListenAndServe(":" + *flagPort, mux))

	results = append(results, time.Now().Format(time.RFC3339))

	mux := http.NewServeMux()
	mux.HandleFunc("/", PostHandler)
	go touch()
	log.Fatal(http.ListenAndServe(":9000", mux))


}