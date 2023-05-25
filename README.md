# Tencent TDMQ-CMQ Go SDK

Manage API: https://cloud.tencent.com/document/product/1496/62819

Data Flow API: https://cloud.tencent.com/document/product/1496/61039

Only support these data flow actions:

- Queue
    - QueryQueueRoute
    - SendMessage
    - BatchSendMessage
    - ReceiveMessage
    - BatchReceiveMessage
    - DeleteMessage
    - BatchDeleteMessage

- Topic
    - QueryTopicRoute
    - PublishMessage
    - BatchPublishMessage

Example:

```shell
go get -u github.com/TencentCloud/tencentcloud-cmq-sdk-go@1.0.0
```

```go
package main

import (
    "fmt"
    "time"

    tcmq "github.com/TencentCloud/tencentcloud-cmq-sdk-go"
)

func main() {
    // get your own secretId/secretKey: https://console.cloud.tencent.com/cam/capi
    client, err := tcmq.NewClient("https://cmq-gz.public.tencenttdmq.com", "AKIDxxxxx", "xxxxx", 5*time.Second)
    if err != nil {
        fmt.Println("new TDMQ-CMQ client", err)
        return
    }
    // client.AppId = 12345  // for privatization request without authentication
    // client.Method = `GET` // default: POST
    // client.Token = `your_token` // for temporary secretId/secretKey auth with token
    client.Debug = false // verbose print each request

    queue := &tcmq.Queue{
        Client:             client,
        Name:               `queue0`, //修改成页面上创建的队列名
        DelaySeconds:       0,
        PollingWaitSeconds: 2,
    }
    resp1, err := queue.Send(`message test 0`)
    if err != nil {
        fmt.Println("send message error:", err)
        return
    }
    fmt.Println("Send Message Status:", resp1.StatusCode())
    fmt.Println("Send Message Response:", resp1)

    respMsg, err := queue.Receive()
    if err != nil {
        fmt.Println("receive message error :", err)
        return
    }
    fmt.Println("Receive Message Response:", respMsg)

    resp2, err := queue.Delete(respMsg.Handle())
    if err != nil {
        fmt.Println("delete message error:", err)
        return
    }
    fmt.Println("Delete Message Response:", resp2)

    resp3, err := queue.BatchSend("a", "b", "c")
    if err != nil {
        fmt.Println("batch send message error :", err)
        return
    }
    fmt.Println("Batch Response:", resp3)

    resp4, err := queue.BatchReceive(5)
    if err != nil {
        fmt.Println("batch receive message:", err)
        return
    }
    fmt.Println("Batch Receive Message Response:", resp4)
    var handles []string
    for _, msg := range resp4.MsgInfos() {
        if len(msg.Handle()) > 0 {
            handles = append(handles, msg.Handle())
        }
    }

    if len(handles) > 0 {
        res, err := queue.BatchDelete(handles...)
        if err != nil {
            fmt.Println("batch delete message error:", err)
            return
        }
        fmt.Println("Batch Delete Message Response:", res)
    }

    topic := &tcmq.Topic{
        Client:     client,
        Name:       `topic0`, // 修改为页面上创建的 topic
        RoutingKey: ``,
        Tags:       nil,
    }
    resp5, err := topic.Publish(`message test 1`)
    if err != nil {
        fmt.Println("publish message error:", err)
        return
    }
    fmt.Println("Publish Message Response:", resp5)

    resp6, err := topic.BatchPublish("x", "y", "z")
    if err != nil {
        fmt.Println("publish message:", err)
        return
    }
    fmt.Println("Batch Publish Message Response:", resp6)
}
```
