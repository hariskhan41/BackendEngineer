package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

type covid struct {
	Date                        string `json:"date"`
	Commulative_tests_positive  string `json:"positive"`
	Commulative_tests_performed string `json:"tests"`
	Expired                     string `json:"expired"`
	Still_admitted              string `json:"admitted"`
	Discharged                  string `json:"discharged"`
	Region                      string `json:"region"`
}

var cov = Load_Covid("covid_final_data.csv")

func main() {
	var addr string
	var network string

	flag.StringVar(&addr, "e", ":4040", "service endpoint [ip addr or socket path]")
	flag.StringVar(&network, "n", "tcp", "network protocol [tcp,unix]")
	flag.Parse()

	// validate supported network protocols
	switch network {
	case "tcp", "tcp4", "tcp6", "unix":
	default:
		log.Fatalln("unsupported network protocol:", network)
	}

	// create a listener for provided network and host address
	ln, err := net.Listen(network, addr)
	if err != nil {
		log.Fatal("failed to create listener:", err)
	}
	defer ln.Close()
	log.Println("**** Global Currency Service ***")
	log.Printf("Service started: (%s) %s\n", network, addr)

	// connection-loop - handle incoming requests
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			if err := conn.Close(); err != nil {
				log.Println("failed to close listener:", err)
			}
			continue
		}
		log.Println("Connected to", conn.RemoteAddr())

		go handleConnection(conn)
	}
}

func parse_input(inp string) (bool, string) {
	splited_string := strings.Split(inp, " ")
	if len(splited_string) != 3 {
		return false, ""
	}
	flag := false
	var key string = ""

	if splited_string[1] == "{\"region\":" || splited_string[1] == "{\"date\":" {
		flag = true
		key = strings.Replace(splited_string[2], "\"", "", -1)

		key = strings.Replace(key, "}", "", -1)

		if splited_string[1] == "{\"date\":" {
			split_date := strings.Split(key, "-")
			if len(split_date) == 3 {
				temp := split_date[0]
				split_date[0] = split_date[2]
				split_date[2] = temp
				key = split_date[0] + "/" + split_date[1] + "/" + split_date[2]
			} else {
				flag = false
				key = ""
			}
		}
	} else {
		flag = false
		key = ""
	}
	return flag, key
}

func handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("error closing connection:", err)
		}
	}()

	if _, err := conn.Write([]byte("Connected...\nUsage: <{\"query\": {\"region\": \"Sindh\"}} OR {\"query\": {\"date\": \"2020-03-20\"}}>\n")); err != nil {
		log.Println("error writing:", err)
		return
	}

	// loop to stay connected with client until client breaks connection
	for {
		// buffer for client command
		cmdLine := make([]byte, (1024 * 4))
		n, err := conn.Read(cmdLine)
		if n == 0 || err != nil {
			log.Println("connection read error:", err)
			return
		}
		flag, str := parse_input(string(cmdLine[0:n]))

		if flag == true {
			str = strings.TrimSuffix(str, "\n")
			result := search_details(cov, str)

			if len(result) == 0 {
				conn.Write([]byte("Nothing Found\n"))
			} else {
				conn.Write([]byte("{\"response\": [\n"))
				for _, data := range result {
					res4, _ := json.MarshalIndent(data, "", "	")
					_, err := conn.Write([]byte(string(res4) + ",\n"))
					if err != nil {
						log.Println("failed to write response:", err)
						return
					}
				}
				conn.Write([]byte("]}"))
				conn.Write([]byte("\n"))
			}
		} else {
			conn.Write([]byte("Invalid command\n"))
			conn.Write([]byte("Connected...\nUsage: GET <{\"query\": {\"region\": \"Sindh\"}}>\n"))
		}
	}
}

// Search
func search_details(table []covid, key string) []covid {
	result := make([]covid, 0)
	key = strings.Replace(key, "\n", "", -1)
	for _, data := range table {
		if data.Region == key || data.Date == key {

			if strings.Contains(key, "/") == true {
				temp_split_date := strings.Split(data.Date, "/")
				temp := temp_split_date[0]
				temp_split_date[0] = temp_split_date[2]
				temp_split_date[2] = temp
				data.Date = ""
				data.Date = temp_split_date[0] + "-" + temp_split_date[1] + "-" + temp_split_date[2]
			}

			result = append(result, data)
		}
	}
	return result
}

// Load data in a structure
func Load_Covid(path string) []covid {
	table := make([]covid, 0)
	file, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	reader := csv.NewReader(file)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err.Error())
		}
		cov := covid{
			Commulative_tests_positive:  row[0],
			Commulative_tests_performed: row[1],
			Date:                        row[2],
			Discharged:                  row[3],
			Expired:                     row[4],
			Region:                      row[5],
			Still_admitted:              row[6],
		}

		table = append(table, cov)
	}
	return table
}
