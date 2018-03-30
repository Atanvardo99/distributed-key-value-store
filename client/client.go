package main

import(
	"net/http"
	"encoding/json"
	"log"
	"bytes"
	"time"
	"flag"
	"fmt"
)

const proxy string = "http://localhost:9000"
const identification string = "00000"

var results []string
var id string

// standard request structure
type Reqs struct{

	Reqtype string
	M map[string]string
	TimeStamp time.Time
	Identify string
}

// nosql
func add(key string, value string) (s string, returnMap map[string]string,  err error){
	//var m map[string] string
	var m = make(map[string]string)

	m[key] = value
	s,returnMap, err = exec("add", m)
	return s,returnMap, err
}
func addmap(m map[string] string) (s string, returnMap map[string]string,  err error){

	s,returnMap, err = exec("add", m)
	return s,returnMap, err
}
func remove(key string) (s string, returnMap map[string]string,  err error){
	var m = make(map[string]string)
	m[key] = ""
	s,returnMap, err = exec("remove", m)
	return s,returnMap, err

}
func modify(key string, value string) (s string, returnMap map[string]string,  err error){
	var m = make(map[string]string)
	m[key] = value
	s,returnMap, err = exec("modify", m)
	return s,returnMap, err
}
func choose(key string) (s string, returnMap map[string]string,  err error){
	var m = make(map[string]string)
	m[key] = ""
	s,returnMap, err = exec("choose", m)
	return s,returnMap, err
}

// req post
func exec(reqtype string, m map[string]string) (s string, returnMap map[string]string,  err error){


	jsonBody := Reqs{reqtype,m, time.Now(),  id}
	encode := new(bytes.Buffer)
	json.NewEncoder(encode).Encode(jsonBody)
	client := http.Client{}
	//url := url.URL{Host:"localhost:9001"}
	request, err := http.NewRequest("POST", proxy, encode)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("post: add ", jsonBody)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln(err)
	}
	var resps Reqs
	json.NewDecoder(resp.Body).Decode(&resps)
	log.Println("get response:", resps)
	// TODO: parse responses
	s, returnMap = parseReq(resps)
	log.Println(s)
	return s, returnMap,nil
}

func parseReq(resp Reqs) (string, map[string]string){

	return resp.Reqtype, resp.M

}

func init() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	id = identification
	flag.Parse()
}

func main(){

	add("123", "12345")
	addmap(map[string]string{"qwer":"qwert", "qw": "qwer"})

	_, m, _ :=choose("123")
	fmt.Println(m["123"])
	//fmt.Println(remove("qwer"))

}