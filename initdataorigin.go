package initdataorigin

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/kpango/glg"
	"github.com/nitishm/go-rejson"
	"github.com/stmcore/digestauth"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Server map data from origin api resp
type Server struct {
	Hostname string `bson:"Hostname"`
	IP       string `bson:"IP"`
	Rack     string `bson:"Rack"`
	Site     string `bson:"Site"`
	Active   bool   `bson:"Active"`
	Type     string `bson:"Type"`
	Vhost    VHosts
}

//DataOrigins map data from origin api resp
type DataOrigins struct {
	Data map[string][]DataOrigin
}

//DataOrigin map data from origin api resp
type DataOrigin struct {
	Hostname            string
	IP                  string
	Rack                string
	Type                string
	MessagesInBytesRate float32
	VHost               string
	App                 string
	AppInstance         string
	ChannelName         string
	Code                string
	FileStream          string
	TimeStamp           time.Time
	Uptime              int
	BytesIn             int
	BytesInRate         int
}

//VHosts map data from origin api resp
type VHosts struct {
	VHosts              []VHost `xml:"VHost"`
	MessagesInBytesRate float32 `xml:"MessagesInBytesRate"`
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
	Name        string `xml:"Name"`
	CurrentStat CurrentIncomingStreamStatistics
}

//CurrentIncomingStreamStatistics map data from origin api resp
type CurrentIncomingStreamStatistics struct {
	Name        string `xml:"Name"`
	UpTime      int    `xml:"Uptime"`
	BytesIn     int    `xml:"BytesIn"`
	BytesInRate int    `xml:"BytesInRate"`
}

//OriginStream map between channel name and stream name
type OriginStream struct {
	ChannelName string `bson:"ChannelName"`
	StreamName  string `bson:"StreamName"`
}

//Username for login origin
var Username = "sysadm"

//Pattern for login origin
var Pattern = "C0rE#"

var dataServers []Server
var dataStreams []OriginStream

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

		dataori.Data[chName][index].Uptime = stat.UpTime
		dataori.Data[chName][index].BytesInRate = stat.BytesInRate
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

	dataori.Data[key][index].Uptime = stat.UpTime
	dataori.Data[key][index].BytesInRate = stat.BytesInRate
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

		wg.Wait()
	}

}

func (dataori *DataOrigins) UpdateByteInAllChannelsInit(dataOri map[string][]DataOrigin) {

	wg := &sync.WaitGroup{}
	for key, value := range dataOri {

		for index, v := range value {

			wg.Add(1)
			arrIP := strings.Split(v.IP, ".")
			lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

			url := "http://" + v.IP + ":8087/v2/servers/" + v.Hostname + "/vhosts/" + v.VHost + "/applications/" + v.App + "/instances/" + v.AppInstance + "/incomingstreams/" + v.FileStream + "/monitoring/current"
			username := Username
			password := Pattern + lastTwoIP

			//go dataori.callOriginAPI(url, username, password, key, index, wg)

			go func() {
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

				dataOri[key][index].Uptime = stat.UpTime
				dataOri[key][index].BytesInRate = stat.BytesInRate
				dataOri[key][index].BytesIn = stat.BytesIn
				dataOri[key][index].TimeStamp = time.Now()
			}()

			if index%100 == 0 {
				wg.Wait()
			}

		}

		wg.Wait()
	}

	dataori.Data = dataOri

}

//LoadDataFromMongo preload streams data and hosts data before process other func
func (dataori *DataOrigins) LoadDataFromMongo() {

	dataStreams = nil
	dataServers = nil

	clientOptions := options.Client().ApplyURI(os.Getenv("MONGO_URL"))

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

	clHost := client.Database("stmcore-monitor-config").Collection("hosts")

	//filter := bson.D{{}}

	findOptions := options.Find()
	findOptions.SetLimit(500)

	curHost, err := clHost.Find(context.TODO(), bson.M{"Active": true}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for curHost.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var elem Server
		err := curHost.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		if elem.Active == true {
			dataServers = append(dataServers, elem)
		}

	}

	if err := curHost.Err(); err != nil {
		log.Fatal(err)
	}

	clStream := client.Database("stmcore-monitor-config").Collection("streams")

	curStream, err := clStream.Find(context.TODO(), bson.M{}, findOptions)
	if err != nil {
		log.Fatal(err)
	}

	for curStream.Next(context.TODO()) {

		// create a value into which the single document can be decoded
		var elem OriginStream
		err := curStream.Decode(&elem)
		if err != nil {
			log.Fatal(err)
		}

		dataStreams = append(dataStreams, elem)
	}

	if err := curHost.Err(); err != nil {
		log.Fatal(err)
	}

	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}

//GetServers get servers from hosts.json
func (dataori *DataOrigins) GetServers() []Server {
	return dataServers
}

//GetStreams get streams from streams.json
func (dataori *DataOrigins) GetStreams() []OriginStream {
	return dataStreams
}

//getChannelFormStream get channel from stream name
func (dataori *DataOrigins) getChannelFormStream(streamName string) string {

	streams := dataori.GetStreams()

	for _, stream := range streams {
		if strings.Split(streamName, "_")[0] == "rdo" && strings.Split(streamName, ".")[0] == stream.StreamName {
			return stream.ChannelName
		}
		if strings.Split(streamName, "_")[0] == stream.StreamName {
			return stream.ChannelName
		}
	}

	return "Unknown"

}

func getCodeFromStreamName(streamName string) string {
	temp := strings.Split(streamName, "_")
	if temp[0] == "rdo" && len(temp) > 1 {
		return strings.Split(streamName, ".")[0]
	}
	return temp[0]
}

func checkStreamActive(ip, hostname, vhost, app, appInstant, fileStream string) bool {
	arrIP := strings.Split(ip, ".")
	lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

	url := "http://" + ip + ":8087/v2/servers/" + hostname + "/vhosts/" + vhost + "/applications/" + app + "/instances/" + appInstant + "/incomingstreams/" + fileStream
	username := Username
	password := Pattern + lastTwoIP

	var digest digestauth.Digest
	data, err := digest.GetInfo(url, username, password, "GET")

	if err != nil && data == nil {
		return false
	}

	return true

}

func getRedisJSON(hostname, addr string, rh *rejson.Handler) (server Server, err error) {
	// Redigo Client

	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer func() {
		//_, err = conn.Do("FLUSHALL")
		err = conn.Close()
		if err != nil {
			return
		}
	}()
	rh.SetRedigoClient(conn)
	glg.Info("executing JSONGET for Redigo Client:", hostname)

	serverJSON, err := redis.Bytes(rh.JSONGet(hostname, "."))
	if err != nil {

		return
	}

	server = Server{}
	err = json.Unmarshal(serverJSON, &server)
	if err != nil {

		return
	}

	return
}

//Init initial data
func (dataori *DataOrigins) Init() {
	//var digest digestauth.Digest
	dataorigin := make(map[string][]DataOrigin)

	rh := rejson.NewReJSONHandler()
	redisURL := os.Getenv("REDIS_URL")

	dataori.LoadDataFromMongo()

	for _, host := range dataServers {
		// arrIP := strings.Split(server.IP, ".")
		// lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

		//glg.Info("Getting connectioncounts from: " + host.Hostname + "(" + host.IP + ")" + " ...")
		// data, err := digest.GetInfo("http://"+server.IP+":8086/connectioncounts", Username, Pattern+lastTwoIP, "GET")

		// if err != nil {
		// 	fmt.Println(err)
		// }

		server, err := getRedisJSON(host.Hostname, redisURL, rh)

		if err != nil {
			glg.Warn("get redis for:", host.Hostname+"("+host.IP+")", err)
		} else {
			if len(server.Vhost.VHosts) == 0 {
				glg.Warn("vhost for", host.Hostname+"("+host.IP+"):", "is empty")
			}
			for _, vhost := range server.Vhost.VHosts {
				for _, application := range vhost.Applications {
					for _, applicationInstance := range application.ApplicationInstances {
						for _, stream := range applicationInstance.Streams {

							// active := checkStreamActive(server.IP, server.IP, vhost.Name, application.Name, applicationInstance.Name, stream.Name)

							// log.Println("Check stream", stream.Name, "active:", active)

							if stream.Name == stream.CurrentStat.Name {
								chname := dataori.getChannelFormStream(stream.Name)

								dataorigin[chname] = append(dataorigin[chname], DataOrigin{
									Hostname:            host.Hostname,
									IP:                  host.IP,
									Rack:                host.Rack,
									Type:                host.Type,
									MessagesInBytesRate: server.Vhost.MessagesInBytesRate,
									VHost:               vhost.Name,
									App:                 application.Name,
									AppInstance:         applicationInstance.Name,
									ChannelName:         chname,
									Code:                getCodeFromStreamName(stream.Name),
									FileStream:          stream.Name,
									Uptime:              stream.CurrentStat.UpTime,
									BytesIn:             stream.CurrentStat.BytesIn,
									BytesInRate:         stream.CurrentStat.BytesInRate,
									TimeStamp:           time.Now(),
								})
							}

							//}

						}
					}
				}
			}
		}

		dataori.Data = dataorigin

	}
	//dataori.UpdateByteInAllChannelsInit(dataorigin)
}
