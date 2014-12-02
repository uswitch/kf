package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/uswitch/kafkazk"
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
	clientId      = kingpin.Flag("clientid", "client id").Default("kf").Short('c').String()
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

	client, err := sarama.NewClient(*clientId, brokerString, nil)
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
		latestOffset, err := client.GetOffset(*topic, int32(*partition), sarama.LatestOffsets)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(latestOffset)
		os.Exit(0)
	}

	config := sarama.NewConsumerConfig()
	config.OffsetMethod = sarama.OffsetMethodNewest

	if *fromBeginning {
		config.OffsetMethod = sarama.OffsetMethodOldest
	} else if *offset > 0 {
		config.OffsetMethod = sarama.OffsetMethodManual
		config.OffsetValue = *offset
	}

	consumer, err := sarama.NewConsumer(client, *topic, int32(*partition), *group, config)
	if err != nil {
		log.Fatal(err)
	}

	if *verbose {
		log.Println("consumer ready")
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
		case event := <-consumer.Events():
			if event.Err != nil {
				log.Println(event.Err)
			} else {
				if *verbose {
					log.Println("offset", event.Offset)
				}
				fmt.Println(string(event.Value[:]))
				msgCount++
				if *max != 0 && msgCount >= *max {
					return
				}
			}
		case <-stop:
			if *verbose {
				log.Println("stopping")
			}
			return
		}
	}
}
