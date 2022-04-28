package mq

import (
	"errors"
	"fmt"
	"github.com/miaocansky/go-mq/rabbitmq"
	"strconv"
	"testing"
	"time"
)

type RecvPro struct {
}

//// 实现消费者 消费消息失败 自动进入延时尝试  尝试3次之后入库db
/*
返回值 error 为nil  则表示该消息消费成功
否则消息会进入ttl延时队列  重复尝试消费3次
3次后消息如果还是失败 消息就执行失败  进入告警 FailAction
*/
func (t *RecvPro) Consumer(msg CustomerMsg) error {
	time.Sleep(time.Second * 1)
	//return errors.New("顶顶顶顶")
	fmt.Println(string(msg.Body))
	//time.Sleep(1*time.Second)
	return errors.New("顶顶顶顶")
	//return nil
}

//消息已经消费3次 失败了 请进行处理
/*
如果消息 消费3次后 仍然失败  此处可以根据情况 对消息进行告警提醒 或者 补偿  入库db  钉钉告警等等
*/
func (t *RecvPro) FailAction(err error, msg CustomerMsg) error {
	fmt.Println(string(msg.Body))
	fmt.Println(err)
	fmt.Println("任务处理失败了，我要进入db日志库了")
	fmt.Println("任务处理失败了，发送钉钉消息通知主人")
	return nil
}

func TestPrduct(t *testing.T) {
	for i := 0; i < 8; i++ {
		msg := "这是一条普通消息" + strconv.Itoa(i)
		SendDelayMessage(msg)
		time.Sleep(1 * time.Second)
		//fmt.Println(i)
	}

}

func ConsumeDelay() {

	config := Config{
		Username:     "jenkin",
		Password:     "123456",
		Host:         "127.0.0.1",
		Port:         "5672",
		Path:         "/",
		ExchangeName: "yoyo_exchange_2",
		RouteKey:     "yoyo_route_2",
		QueueName:    "yoyo_queue_2",
		RetryNum:     0,
	}

	rabbit, err := rabbitmq.NewRabbitMQ(config)
	defer rabbit.Close()
	if err != nil {

	}
	processTask := &RecvPro{}
	// 执行消费
	rabbit.Consumer(processTask)

}

func SendMessage() {
	config := Config{
		Username:     "jenkin",
		Password:     "123456",
		Host:         "127.0.0.1",
		Port:         "5672",
		Path:         "/",
		ExchangeName: "yoyo_exchange",
		RouteKey:     "yoyo_route",
		QueueName:    "yoyo_queue",
		RetryNum:     0,
	}
	rabbit, err := rabbitmq.NewRabbitMQ(config)
	defer rabbit.Close()
	if err != nil {

	}
	message := rabbit.SendMessage(Message{Body: "这是一条普通消息"}, true)
	fmt.Println(message)

}

func SendDelayMessage(msg string) {

	config := Config{
		Username:     "jenkin",
		Password:     "123456",
		Host:         "127.0.0.1",
		Port:         "5672",
		Path:         "/",
		ExchangeName: "yoyo_exchange_2",
		RouteKey:     "yoyo_route_2",
		QueueName:    "yoyo_queue_2",
		RetryNum:     0,
	}

	rabbit, err := rabbitmq.NewRabbitMQ(config)
	defer rabbit.Close()
	if err != nil {

	}
	rabbit.SendDelayMessage(Message{Body: msg, DelayTime: 5}, true)
	//fmt.Println(message)

}
