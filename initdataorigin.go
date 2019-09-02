package initdataorigin

import (
	"context"
	"encoding/xml"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/stmcore/digestauth"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Server map data from origin api resp
type Server struct {
	Hostname string `json:"Hostname"`
	IP       string `json:"IP"`
	Rack     string `json:"Rack"`
	Site     string `json:"Site"`
}

//DataOrigins map data from origin api resp
type DataOrigins struct {
	Data map[string][]DataOrigin
}

//DataOrigin map data from origin api resp
type DataOrigin struct {
	Hostname    string
	IP          string
	Rack        string
	VHost       string
	App         string
	AppInstance string
	ChannelName string
	Code        string
	FileStream  string
	TimeStamp   time.Time
	BytesIn     int
}

//VHosts map data from origin api resp
type VHosts struct {
	WowzaStreamingEngine xml.Name `xml:"WowzaStreamingEngine"`
	VHosts               []VHost  `xml:"VHost"`
}

//VHost map data from origin api resp
type VHost struct {
	Name         string        `xml:"Name"`
	Applications []Application `xml:"Application"`
}

//Application map data from origin api resp
type Application struct {
	Name                 string                `xml:"Name"`
	ApplicationInstances []ApplicationInstance `xml:"ApplicationInstance"`
}

//ApplicationInstance map data from origin api resp
type ApplicationInstance struct {
	Name    string   `xml:"Name"`
	Streams []Stream `xml:"Stream"`
}

//Stream map data from origin api resp
type Stream struct {
	Name string `xml:"Name"`
}

//CurrentIncomingStreamStatistics map data from origin api resp
type CurrentIncomingStreamStatistics struct {
	CurrentIncomingStreamStatistics xml.Name `xml:"CurrentIncomingStreamStatistics"`
	Name                            string   `xml:"Name"`
	BytesIn                         int      `xml:"BytesIn"`
}

//OriginStream map between channel name and stream name
type OriginStream struct {
	ChannelName string `json:"ChannelName"`
	StreamName  string `json:"StreamName"`
}

//Username for login origin
var Username = "sysadm"

//Pattern for login origin
var Pattern = "C0rE#"

var Streams []OriginStream

//UpdateByteInByChannel interval update data from origin api
func (dataori *DataOrigins) UpdateByteInByChannel(chName string) {
	var digest digestauth.Digest
	for index, v := range dataori.Data[chName] {
		arrIP := strings.Split(v.IP, ".")
		lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

		url := "http://" + v.IP + ":8087/v2/servers/" + v.Hostname + "/vhosts/" + v.VHost + "/applications/" + v.App + "/instances/" + v.AppInstance + "/incomingstreams/" + v.FileStream + "/monitoring/current"
		data, err := digest.GetInfo(url, Username, Pattern+lastTwoIP, "GET")

		if err != nil {
			//log.Println(err)
		}

		var stat CurrentIncomingStreamStatistics
		xml.Unmarshal([]byte(data), &stat)

		dataori.Data[chName][index].BytesIn = stat.BytesIn
		dataori.Data[chName][index].TimeStamp = time.Now()

	}
}

//callOriginAPI call origin api
func (dataori *DataOrigins) callOriginAPI(url, username, password, key string, index int, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	var digest digestauth.Digest
	data, err := digest.GetInfo(url, username, password, "GET")

	if err != nil {
		//log.Println(err)
	}

	var stat CurrentIncomingStreamStatistics
	xml.Unmarshal([]byte(data), &stat)

	dataori.Data[key][index].BytesIn = stat.BytesIn
	dataori.Data[key][index].TimeStamp = time.Now()
}

//UpdateByteInAllChannels refesh ByteIn
func (dataori *DataOrigins) UpdateByteInAllChannels() {

	wg := &sync.WaitGroup{}
	for key, value := range dataori.Data {

		for index, v := range value {

			wg.Add(1)
			arrIP := strings.Split(v.IP, ".")
			lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

			url := "http://" + v.IP + ":8087/v2/servers/" + v.Hostname + "/vhosts/" + v.VHost + "/applications/" + v.App + "/instances/" + v.AppInstance + "/incomingstreams/" + v.FileStream + "/monitoring/current"
			username := Username
			password := Pattern + lastTwoIP

			go dataori.callOriginAPI(url, username, password, key, index, wg)

			if index%100 == 0 {
				wg.Wait()
			}

		}
	}

}

func GetDataFromMongo(url, db, collection string, results []interface{}) {
	clientOptions := options.Client().ApplyURI("mongodb://stmcore:stmcore1212312121@10.18.14.107:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	cl := client.Database(db).Collection(collection)

	//filter := bson.D{{}}

	findOptions := options.Find()
	findOptions.SetLimit(200)

	cur, err := cl.Find(context.TODO(), bson.D{{}}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for cur.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var elem interface{}
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		results = append(results, elem)
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(results)

	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}

//GetServers get servers from hosts.json
func (dataori *DataOrigins) GetServers() []Server {
	var data []Server
	// dataServer, err := ioutil.ReadFile("./hosts.json")

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// err = json.Unmarshal([]byte(dataServer), &data)

	clientOptions := options.Client().ApplyURI("mongodb://stmcore:stmcore1212312121@10.18.14.107:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	cl := client.Database("stmcore-monitor-config").Collection("hosts")

	//filter := bson.D{{}}

	findOptions := options.Find()
	findOptions.SetLimit(500)

	cur, err := cl.Find(context.TODO(), bson.D{{}}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for cur.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var elem Server
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		data = append(data, elem)
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")

	return data
}

//GetStreams get streams from streams.json
func (dataori *DataOrigins) GetStreams() []OriginStream {
	var data []OriginStream
	// dataStream, err := ioutil.ReadFile("./streams.json")

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// err = json.Unmarshal([]byte(dataStream), &data)

	clientOptions := options.Client().ApplyURI("mongodb://stmcore:stmcore1212312121@10.18.14.107:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	cl := client.Database("stmcore-monitor-config").Collection("streams")

	//filter := bson.D{{}}

	findOptions := options.Find()
	findOptions.SetLimit(500)

	cur, err := cl.Find(context.TODO(), bson.D{{}}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for cur.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var elem OriginStream
		err := cur.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		data = append(data, elem)
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")

	Streams = data

	return data
}

//getChannelFormStream get channel from stream name
func (dataori *DataOrigins) getChannelFormStream(streamName string) string {

	for _, stream := range Streams {
		if strings.Split(streamName, "_")[0] == stream.StreamName {
			return stream.ChannelName
		}
	}

	return "Unknown"

}

//Init initial data
func (dataori *DataOrigins) Init() {
	var digest digestauth.Digest
	dataorigin := make(map[string][]DataOrigin)

	servers := dataori.GetServers()
	dataori.GetStreams()

	for _, server := range servers {
		arrIP := strings.Split(server.IP, ".")
		lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

		fmt.Println("Getting connectioncounts from: " + server.IP + " ...")
		data, err := digest.GetInfo("http://"+server.IP+":8086/connectioncounts", Username, Pattern+lastTwoIP, "GET")

		if err != nil {
			fmt.Println(err)
		}

		if data != nil {
			var vhosts VHosts
			xml.Unmarshal([]byte(data), &vhosts)

			for _, vhost := range vhosts.VHosts {
				for _, application := range vhost.Applications {
					for _, applicationInstance := range application.ApplicationInstances {
						for _, stream := range applicationInstance.Streams {
							chname := dataori.getChannelFormStream(stream.Name)

							dataorigin[chname] = append(dataorigin[chname], DataOrigin{
								Hostname:    server.Hostname,
								IP:          server.IP,
								Rack:        server.Rack,
								VHost:       vhost.Name,
								App:         application.Name,
								AppInstance: applicationInstance.Name,
								ChannelName: chname,
								Code:        strings.Split(stream.Name, "_")[0],
								FileStream:  stream.Name,
							})
						}
					}
				}
			}
			dataori.Data = dataorigin
		}

	}
}
