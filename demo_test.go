package tdmq

import (
	"fmt"
	"testing"
	"time"
)

// 最简单的场景，发送一条消息，获取一条消息
func TestSyncSingleSendMessageAndSingleReceiveMessage(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	//2. 创建队列
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue3`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	// 3. 发消息
	sendResponse, err := queue.Send("I am a message body.")
	if err != nil {
		fmt.Println("send message error. ", err)
		return
	}

	if sendResponse.Code() == 0 {
		fmt.Printf("【SendMessage】: messageId = %s\n", sendResponse.MsgId())
	} else {
		fmt.Printf("Send message failed. errorCode = %d\n", sendResponse.Code())
		return
	}

	// 4. 收消息。通常情况下，收消息放到一个单独的线程里，一直死循环轮训
	receiveResponse, err := queue.Receive()
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", receiveResponse.MsgId(), receiveResponse.MsgBody())

		// 5. 处理业务逻辑之后，删除消息.注意这里传入 handle，而不是 messageId
		deleteResponse, _ := queue.Delete(receiveResponse.Handle())
		if deleteResponse.Code() == 0 {
			fmt.Printf("【DeleteMessage】: messageId = %s\n", receiveResponse.MsgId())
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}
}

// 批量读写消息的场景，如果一次拉取获取不到足够的消息数，则请求会hold住，等待消息足够
func TestSyncBatchSendMessageAndBatchReceiveMessage(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	//2. 创建队列
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue3`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	// 3. 发送10条消息
	messages := make([]string, 0)
	for i := 0; i < 10; i++ {
		messages = append(messages, fmt.Sprintf("I am a message body. %d", i))
	}

	sendResponse, err := queue.BatchSend(messages...)
	if err != nil {
		fmt.Println("send message error. ", err)
		return
	}
	if sendResponse.Code() == 0 {
		fmt.Printf("【BatchSendMessage】success \n")
	} else {
		fmt.Printf("Send message failed. errorCode = %d\n", sendResponse.Code())
		return
	}

	// 4. 批量获取10条消息，如果消息不够10条，则服务端会夯住请求直到超时时间。通常情况下，收消息放到一个单独的线程里，一直死循环轮训
	receiveResponse, err := queue.BatchReceive(10)
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		for _, message := range receiveResponse.MsgInfos() {
			fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", message.MsgId(), message.MsgBody())

			// 5. 处理业务逻辑之后，删除消息，注意这里传入 handle，而不是 messageId
			deleteResponse, _ := queue.Delete(message.Handle())
			if deleteResponse.Code() == 0 {
				fmt.Printf("【DeleteMessage】: messageId = %s\n", message.MsgId())
			}
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}
}

// 开启两个异步任务批量读写消息的场景，如果一次拉取获取不到足够的消息数，则请求会hold住，等待消息足够
func TestAsyncBatchSendMessageAndBatchReceiveMessage(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	//2. 创建队列
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue3`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			// 3. 发送10条消息
			messages := make([]string, 0)
			for i := 0; i < 10; i++ {
				messages = append(messages, fmt.Sprintf("I am a message body. %d", i))
			}

			sendResponse, err := queue.BatchSend(messages...)
			if err != nil {
				fmt.Println("send message error. ", err)
				return
			}
			if sendResponse.Code() == 0 {
				fmt.Printf("【BatchSendMessage】success \n")
			} else {
				fmt.Printf("Send message failed. errorCode = %d\n", sendResponse.Code())
				return
			}
		}
	}()

	go func() {
		// 4. 批量获取10条消息，如果消息不够10条，则服务端会夯住请求直到超时时间。通常情况下，收消息放到一个单独的线程里，一直死循环轮训
		for {
			receiveResponse, err := queue.BatchReceive(10)
			if err != nil {
				fmt.Println("receive message error. ", err)
				return
			}
			if receiveResponse.Code() == 0 {
				for _, message := range receiveResponse.MsgInfos() {
					fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", message.MsgId(), message.MsgBody())

					// 5. 处理业务逻辑之后，删除消息.注意这里传入 handle，而不是 messageId
					deleteResponse, _ := queue.Delete(message.Handle())
					if deleteResponse.Code() == 0 {
						fmt.Printf("【DeleteMessage】: messageId = %s\n", message.MsgId())
					}
				}
			} else {
				fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
			}
		}
	}()

	time.Sleep(10000 * time.Second)
}

// 延迟消息，消息发到mq之后的3s再投递给消费者
func TestDelayMessage(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	//2. 创建队列
	queue := &Queue{
		Client:             client,
		Name:               "ledou-test-queue3", // 修改成页面上创建的队列名
		DelaySeconds:       0,                   // 消息延迟可见时间
		PollingWaitSeconds: 4,                   // 消费消息长轮询等待时间
	}

	// 3. 发消息，设置延迟消息为3s
	sendResponse, err := client.SendMessage("ledou-test-queue3", "I am a message body.", 3)
	if err != nil {
		fmt.Println("send message error. ", err)
		return
	}

	if sendResponse.Code() == 0 {
		fmt.Printf("【SendMessage】: messageId = %s\n", sendResponse.MsgId())
	} else {
		fmt.Printf("Send message failed. errorCode = %d\n", sendResponse.Code())
		return
	}

	// 4. 收消息，由于消息延迟是3s，所以3s后收到消息
	receiveResponse, err := queue.Receive()
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", receiveResponse.MsgId(), receiveResponse.MsgBody())

		// 5. 删除消息
		deleteResponse, _ := queue.Delete(receiveResponse.Handle())
		if deleteResponse.Code() == 0 {
			fmt.Printf("【DeleteMessage】: messageId = %s\n", receiveResponse.MsgId())
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}

	time.Sleep(5 * time.Second)
}

// 最简单的主题模型样例
func TestSimpleTopic(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	// 2. 创建主题并发送消息
	topic := &Topic{
		Client:     client,
		Name:       `ledou-test-topic`, //
		RoutingKey: ``,
		Tags:       nil,
	}

	// 发送消息
	publishResponse, err := topic.Publish("I am a topic message body.")
	if err != nil {
		fmt.Println("publish message error. ", err)
		return
	}
	if publishResponse.Code() == 0 {
		fmt.Println("【PublishMessage】success")
	}

	//3. 创建队列，并获取主题发布的消息。注意：需要在控制台上把 ledou-test-queue3 队列设置为主题的订阅方才能接收到消息
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue3`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	receiveResponse, err := queue.Receive()
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", receiveResponse.MsgId(), receiveResponse.MsgBody())

		// 5. 处理业务逻辑之后，删除消息.注意这里传入 handle，而不是 messageId
		deleteResponse, _ := queue.Delete(receiveResponse.Handle())
		if deleteResponse.Code() == 0 {
			fmt.Printf("【DeleteMessage】: messageId = %s\n", receiveResponse.MsgId())
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}
}

// 通过 tag 过滤消息的例子。
// 控制台创建的订阅带 tag1 标签。topic 发送的消息只有带有tag1 标签的消息才能被订阅的queue接收到
func TestTopicWithTag(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	// 2. 创建主题并发送消息
	topic := &Topic{
		Client:     client,
		Name:       `ledou-test-topic`, // 主题名称
		RoutingKey: ``,
		Tags:       []string{"tag1"}, // 发送的消息携带tag1标签
	}

	// 发送消息
	publishResponse, err := topic.Publish("I am a topic message body with tag1.")
	if err != nil {
		fmt.Println("publish message error. ", err)
		return
	}
	if publishResponse.Code() == 0 {
		fmt.Println("【PublishMessage】success")
	}

	//3. 创建队列，并获取主题发布的消息。注意：需要在控制台上把 ledou-test-queue4 队列设置为主题的订阅方才能接收到消息，并且设置 tag1 消息过滤条件
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue4`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	receiveResponse, err := queue.Receive()
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", receiveResponse.MsgId(), receiveResponse.MsgBody())

		// 5. 处理业务逻辑之后，删除消息.注意这里传入 handle，而不是 messageId
		deleteResponse, _ := queue.Delete(receiveResponse.Handle())
		if deleteResponse.Code() == 0 {
			fmt.Printf("【DeleteMessage】: messageId = %s\n", receiveResponse.MsgId())
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}
}

// 通过 routeKey 过滤消息的例子。
func TestTopicWithRouteKey(t *testing.T) {
	// 1. 创建 cmq client
	// 替换成实际的接入点以及ak、sk
	client, err := NewClient("", "", "", 5*time.Second)
	if err != nil {
		fmt.Println("create cmq client error.", err)
		return
	}

	fmt.Println("\n\nCreate cmq client success... ...")

	client.Debug = false

	// 2. 创建主题并发送消息
	topic := &Topic{
		Client:     client,
		Name:       `ledou-test-topic-route`, // 主题名称
		RoutingKey: `www.qq.com`,             // 发送的消息携带路由键
		Tags:       nil,
	}

	// 发送消息
	publishResponse, err := topic.Publish("I am a topic message body with route key:website .")
	if err != nil {
		fmt.Println("publish message error. ", err)
		return
	}
	if publishResponse.Code() == 0 {
		fmt.Println("【PublishMessage】success")
	}

	//3. 创建队列，并获取主题发布的消息。注意：需要在控制台上把 ledou-test-queue5 队列设置为主题的订阅方才能接收到消息，并且设置 tag1 消息过滤条件
	queue := &Queue{
		Client:             client,
		Name:               `ledou-test-queue5`, //修改成页面上创建的队列名
		DelaySeconds:       0,                   // 延迟队列的延迟时间
		PollingWaitSeconds: 3,                   // 消费消息长轮询等待时间，建议 3s
	}

	receiveResponse, err := queue.Receive()
	if err != nil {
		fmt.Println("receive message error. ", err)
		return
	}

	if receiveResponse.Code() == 0 {
		fmt.Printf("【ReceiveMessage】: messageId = %s, messageBody = %s\n", receiveResponse.MsgId(), receiveResponse.MsgBody())

		// 5. 处理业务逻辑之后，删除消息.注意这里传入 handle，而不是 messageId
		deleteResponse, _ := queue.Delete(receiveResponse.Handle())
		if deleteResponse.Code() == 0 {
			fmt.Printf("【DeleteMessage】: messageId = %s\n", receiveResponse.MsgId())
		}
	} else {
		fmt.Printf("Receive message failed. errorCode = %d\n", receiveResponse.Code())
	}
}
