package main

import (
	"fmt"
	"github.com/uswitch/kafkazk"
	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/alecthomas/kingpin.v1"
	"log"
	"os"
	"os/signal"
	"sort"
)

var (
	server        = kingpin.Flag("zookeeper", "host:port of zookeeper server (defaults to value of ZOOKEEPER environment variable)").Default(os.Getenv("ZOOKEEPER")).Short('z').String()
	list          = kingpin.Flag("list", "list available topics and quit").Short('l').Bool()
	latest        = kingpin.Flag("latest", "show latest available offset and quit").Short('a').Bool()
	verbose       = kingpin.Flag("verbose", "log informative messages (including offsets)").Short('v').Bool()
	max           = kingpin.Flag("max", "maximum number of messages before stopping (0 = never stop)").Default("0").Short('m').Int()
	fromBeginning = kingpin.Flag("from-beginning", "get messages from the beginning").Short('b').Bool()
	offset        = kingpin.Flag("offset", "get messages from this offset (0 = get only new messages)").Default("0").Short('o').Int64()
	group         = kingpin.Flag("group", "consumer group").Default("kf").Short('g').String()
	clientID      = kingpin.Flag("clientid", "client id").Default("kf").Short('c').String()
	partition     = kingpin.Flag("partition", "partition").Default("0").Short('p').Int()
	topic         = kingpin.Arg("topic", "topic to listen for").String()
)

func main() {
	kingpin.Parse()

	if *server == "" {
		fmt.Fprintln(os.Stderr, "no zookeeper given")
		os.Exit(1)
	}

	brokers, err := kafkazk.LookupBrokers(*server)
	if err != nil {
		log.Fatal(err)
	}

	brokerString := make([]string, len(brokers))
	for i, b := range brokers {
		brokerString[i] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}

	if *verbose {
		log.Println("connecting to kafka, using brokers from zookeeper:", brokerString)
	}

	clientConfig := sarama.NewConfig()
	clientConfig.ClientID = *clientID
	client, err := sarama.NewClient(brokerString, clientConfig)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	if *verbose {
		log.Println("connected")
	}

	topics, err := client.Topics()
	if err != nil {
		log.Fatal(err)
	}

	sort.Strings(topics)

	if *list {
		for _, topic := range topics {
			fmt.Println(topic)
		}
		os.Exit(0)
	}

	if *topic == "" {
		log.Fatal("no topic given")
	}

	index := sort.SearchStrings(topics, *topic)
	found := index < len(topics) && topics[index] == *topic
	if !found {
		log.Fatal("no such topic")
		os.Exit(1)
	}

	if *latest {
		latestOffset, err := client.GetOffset(*topic, int32(*partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(latestOffset)
		os.Exit(0)
	}

	master, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}

	if *verbose {
		log.Println("master consumer ready")
	}

	consumeFrom := sarama.OffsetNewest
	if *fromBeginning {
		consumeFrom = sarama.OffsetOldest
	}
	if *offset > 0 {
		consumeFrom = *offset
	}

	consumer, err := master.ConsumePartition(*topic, int32(*partition), consumeFrom)
	if err != nil {
		log.Fatal(err)
	}

	if *verbose {
		log.Println("partition consumer ready")
	}
	defer consumer.Close()

	msgCount := 0

	defer func() {
		if *verbose {
			log.Println("got", msgCount, "message(s)")
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	for {
		select {
		case error := <-consumer.Errors():
			log.Println(error)
		case message := <-consumer.Messages():
			if *verbose {
				log.Println("offset", message.Offset)
			}
			fmt.Println(string(message.Value[:]))
			msgCount++
			if *max != 0 && msgCount >= *max {
				return
			}
		case <-stop:
			if *verbose {
				log.Println("stopping")
			}
			return
		}
	}
}
