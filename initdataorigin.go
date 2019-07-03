package initdataorigin

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/stmcore/digestauth"
)

type Server struct {
	Hostname string `json: "Hostname"`
	IP       string `json: "IP"`
	Rack     string `json: "Rack"`
}

type DataOrigins struct {
	Data map[string][]DataOrigin
}

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

type VHosts struct {
	WowzaStreamingEngine xml.Name `xml:"WowzaStreamingEngine"`
	VHosts               []VHost  `xml:"VHost"`
}

type VHost struct {
	Name         string        `xml:"Name"`
	Applications []Application `xml:"Application"`
}

type Application struct {
	Name                 string                `xml:"Name"`
	ApplicationInstances []ApplicationInstance `xml:"ApplicationInstance"`
}

type ApplicationInstance struct {
	Name    string   `xml:"Name"`
	Streams []Stream `xml:"Stream"`
}

type Stream struct {
	Name string `xml:"Name"`
}

type CurrentIncomingStreamStatistics struct {
	CurrentIncomingStreamStatistics xml.Name `xml:"CurrentIncomingStreamStatistics"`
	Name                            string   `xml:"Name"`
	BytesIn                         int      `xml:BytesIn`
}

type OriginStream struct {
	ChannelName string `json: "ChannelName"`
	StreamName  string `json: "StreamName"`
}

func (self *DataOrigins) UpdateByteInByChannel(chName string) {
	var digest digestauth.Digest
	for index, v := range self.Data[chName] {
		arrIP := strings.Split(v.IP, ".")
		lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

		url := "http://" + v.IP + ":8087/v2/servers/" + v.Hostname + "/vhosts/" + v.VHost + "/applications/" + v.App + "/instances/" + v.AppInstance + "/incomingstreams/" + v.FileStream + "/monitoring/current"
		data, err := digest.GetInfo(url, "sysadm", "1down2go@"+lastTwoIP, "GET")

		if err != nil {
			//log.Println(err)
		}

		var stat CurrentIncomingStreamStatistics
		xml.Unmarshal([]byte(data), &stat)

		self.Data[chName][index].BytesIn = stat.BytesIn

	}
}

func (self *DataOrigins) callOriginAPI(url, username, password, key string, index int, wg *sync.WaitGroup) {
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

	self.Data[key][index].BytesIn = stat.BytesIn
}

func (self *DataOrigins) UpdateByteInAllChannels() {

	wg := &sync.WaitGroup{}
	for key, value := range self.Data {

		for index, v := range value {

			wg.Add(1)
			arrIP := strings.Split(v.IP, ".")
			lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

			url := "http://" + v.IP + ":8087/v2/servers/" + v.Hostname + "/vhosts/" + v.VHost + "/applications/" + v.App + "/instances/" + v.AppInstance + "/incomingstreams/" + v.FileStream + "/monitoring/current"
			username := "sysadm"
			password := "1down2go@" + lastTwoIP

			go self.callOriginAPI(url, username, password, key, index, wg)

			if index%100 == 0 {
				wg.Wait()
			}

		}
	}

}
func (self *DataOrigins) GetServers() []Server {
	var data []Server
	dataServer, err := ioutil.ReadFile("./hosts.json")

	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal([]byte(dataServer), &data)
	return data
}

func (self *DataOrigins) GetStreams() []OriginStream {
	var data []OriginStream
	dataStream, err := ioutil.ReadFile("./streams.json")

	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal([]byte(dataStream), &data)
	return data
}

func (self *DataOrigins) getChannelFormStream(streamName string) string {

	streams := self.GetStreams()

	for _, stream := range streams {
		if strings.Split(streamName, "_")[0] == stream.StreamName {
			return stream.ChannelName
		}
	}

	return "Unknown"

}

func (self *DataOrigins) Init() {
	var digest digestauth.Digest
	dataorigin := make(map[string][]DataOrigin)

	servers := self.GetServers()

	for _, server := range servers {
		arrIP := strings.Split(server.IP, ".")
		lastTwoIP := strings.Join(arrIP[len(arrIP)-2:], ".")

		data, err := digest.GetInfo("http://"+server.IP+":8086/connectioncounts", "sysadm", "1down2go@"+lastTwoIP, "GET")

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
							chname := self.getChannelFormStream(stream.Name)

							dataorigin[chname] = append(dataorigin[chname], DataOrigin{
								Hostname:    server.Hostname,
								IP:          server.IP,
								Rack:        server.Rack,
								VHost:       vhost.Name,
								App:         application.Name,
								AppInstance: applicationInstance.Name,
								ChannelName: chname,
								FileStream:  stream.Name,
								TimeStamp:   time.Now(),
							})
						}
					}
				}
			}
			self.Data = dataorigin
		}

	}
}
