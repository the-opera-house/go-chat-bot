/*
*
* Name:     Twitch IRC Listener
* Sys Name: twitch-irc
* Author:   Nifty255
*
*/

package main

import (
  "fmt"                         // Prints to console.
  "time"                        // Timing related functions.
  "regexp"                      // Regular Expression functions.
  "strings"                     // String manipulation functions.
  "strconv"                     // String/Number conversion functions.
  "crypto/tls"                  // Web security functions and structures.
  "github.com/koding/kite"      // Microservice functions and structures.
  "github.com/thoj/go-ircevent" // IRC client functions and structures.
  ogcl "github.com/the-opera-house/go-common-lib/common"
  ogcn "github.com/the-opera-house/go-common-lib/net"
  ogdm "github.com/the-opera-house/go-common-lib/models"
)

type IRCDriver struct {
  
  DbDriver        *ogcn.DatabaseDriver
  ConnectTicker   *time.Ticker
  ConnectQueue    *ogdm.StringQueue
  ListenerPool    []Listener
  ChattersTicker  *time.Ticker
  ActiveChatters  []ogdm.ChattersBatch
  BufferTicker    *time.Ticker
  BufferEvents    *ogdm.EventQueue
  BufferChat      *ogdm.StringQueue
  KiteManager     *kite.Kite
  EventClient     *kite.Client
  ChatClient      *kite.Client
  EventConnected  bool
  ChatConnected   bool
  Channels        map[string]*ogdm.IdentitySlim
  IsPrimary       bool
}

func CreateIrcDriver(d *ogcn.DatabaseDriver, k *kite.Kite) *IRCDriver {
  
  e := k.NewClient("http://" +
                   config.Addresses.Event +
                   ":" +
                   strconv.Itoa(config.Ports.Event) +
                   "/kite")
  c := k.NewClient("http://" +
                   config.Addresses.Chat +
                   ":" +
                   strconv.Itoa(config.Ports.Chat) +
                   "/kite")
  
  driver := IRCDriver{
    DbDriver: d,
    ConnectTicker: time.NewTicker(time.Second),
    ConnectQueue: ogdm.StringQueueNew(20),
    ListenerPool: make([]Listener, 0, 100),
    ChattersTicker: time.NewTicker(15 * time.Minute ),
    ActiveChatters: make([]ogdm.ChattersBatch, 0, 25000),
    BufferTicker: time.NewTicker(500 * time.Microsecond),
    BufferEvents: ogdm.EventQueueNew(250),
    BufferChat: ogdm.StringQueueNew(2500),
    KiteManager: k,
    EventClient: e,
    ChatClient: c,
    EventConnected: false,
    ChatConnected: false,
    Channels: make(map[string]*ogdm.IdentitySlim, 500000),
    IsPrimary: false }
  
  e.OnConnect(driver.EventConnect)
  e.OnDisconnect(driver.EventDisconnect)
  c.OnConnect(driver.ChatConnect)
  c.OnDisconnect(driver.ChatDisconnect)
  
  e.DialForever()
  c.DialForever()
  
  // Chat Handler health ticker
  go func() {
    
    ticker := time.NewTicker(250 * time.Millisecond)
    for {
      
      <- ticker.C
      
      if (driver.ChatConnected && driver.IsPrimary) {
        
        driver.ChatClient.Tell("im-here", true)
      }
    }
  }()
  
  // Listener connection queue
  go func() {
    for {
      select {
      case <- driver.ConnectTicker.C:
        
        if (!(driver.ListenerQueuesBusy())) {
          
          nextUp := driver.ConnectQueue.Pop()
          if (nextUp != "") {
            
            for i := 0; i < len(driver.ListenerPool); i++ {
              
              if (driver.ListenerPool[i].Username == nextUp) {
                
                driver.ListenerPool[i].Listen()
                break;
              }
            }
          }
        }
      }
    }
  }()
  
  // Active chatters ticker
  go func() {
    for {
       select {
        case <- driver.ChattersTicker.C:
         if (driver.IsPrimary) {
           
           ogdm.ChattersBatchCreate(driver.DbDriver, driver.ActiveChatters)
         }
         
         driver.ActiveChatters = make([]ogdm.ChattersBatch, 0, 25000)
       }
    }
  }()
  
  // Buffer sender
  go func() {
    for {
       select {
        case <- driver.BufferTicker.C:
         if (driver.EventConnected && driver.BufferEvents.Count > 0) {
           
           event := driver.BufferEvents.Pop()
           driver.EventClient.Tell("process-event", event)
         }
         if (driver.ChatConnected && driver.BufferChat.Count > 0) {
           
           message := driver.BufferChat.Pop()
           driver.ChatClient.Tell("twitch-chatter", message)
         }
       }
    }
  }()
  
  return &driver
}

func (i *IRCDriver) ListenerQueuesBusy() bool {
  
  channelsBusy := 0
  
  for ind := 0; ind < len(i.ListenerPool); ind++ {
    
    if (i.ListenerPool[ind].Listening && i.ListenerPool[ind].ChannelBuffer.Count > 0) {
      
      channelsBusy++
      if (channelsBusy >= 3) { return true }
    }
  }
  
  return false
}

func (i *IRCDriver) EventConnect() {
  
  i.EventConnected = true
  fmt.Println("Connected to Event Handler.")
}

func (i *IRCDriver) ChatConnect() {
  
  i.ChatConnected = true
  fmt.Println("Connected to Chat Handler.")
}

func (i *IRCDriver) EventDisconnect() {
  
  i.EventConnected = false
  fmt.Println("Disconnected from Event Handler.")
  
  i.EventClient.Close()
  i.EventClient = i.KiteManager.NewClient("http://" +
                                          config.Addresses.Event +
                                          ":" +
                                          strconv.Itoa(config.Ports.Event) +
                                          "/kite")
  
  i.EventClient.OnConnect(i.EventConnect)
  i.EventClient.OnDisconnect(i.EventDisconnect)
  
  i.EventClient.DialForever()
}

func (i *IRCDriver) ChatDisconnect() {
  
  i.ChatConnected = false
  fmt.Println("Disconnected from Chat Handler.")
  
  i.ChatClient.Close()
  i.ChatClient = i.KiteManager.NewClient("http://" +
                                         config.Addresses.Chat +
                                         ":" +
                                         strconv.Itoa(config.Ports.Chat) +
                                         "/kite")
  
  i.ChatClient.OnConnect(i.ChatConnect)
  i.ChatClient.OnDisconnect(i.ChatDisconnect)
  
  i.ChatClient.DialForever()
}

func (i *IRCDriver) ListenToChannel(user *ogdm.IdentitySlim) {
  
  existing, exists := i.Channels[user.PlatformID]
  
  if (exists) {
    
    if (existing.Login == user.Login) {
      
      return
    }
    
    fmt.Println("Given channel with recognized ID but different username. Switching channels from \"" + existing.Login +  "\" to \"" + user.Login + "\".")
    
    i.Channels[user.PlatformID] = user
    i.PartChannel(existing.Login)
    timer := time.NewTimer(time.Second)
    go func() {
      
      <-timer.C
      
      for ind := 0; ind < len(i.ListenerPool); ind++ {
        
        if (len(i.ListenerPool[ind].Channels) < config.ChannelsPerListener) {
          
          i.ListenerPool[ind].ChannelBuffer.Push(user)
          break
        }
      }
    }()
    
    return
  }
  
  i.Channels[user.PlatformID] = user
  
  var lastListener *Listener
  
  if (len(i.ListenerPool) == 0) {
    
    username := "Justinfan" + strconv.Itoa(ogcl.SpecificRand(1000, 9999))
    lastListener = CreateListener(username, "", i)
    i.ListenerPool = append(i.ListenerPool, *lastListener)
    i.ConnectQueue.Push(lastListener.Username)
  } else {
    
    lastListener = &(i.ListenerPool[len(i.ListenerPool) - 1])
  }
  
  if ((len(lastListener.Channels) + lastListener.ChannelBuffer.Count) < config.ChannelsPerListener) {
    
    lastListener.ChannelBuffer.Push(user)
  } else {
    
    username := ""
    alreadyExists := true
    for alreadyExists {
      
      alreadyExists = false
      username = "Justinfan" + strconv.Itoa(ogcl.SpecificRand(1000, 9999))
      for j := 0; j < len(i.ListenerPool); j++ {
        
        if (i.ListenerPool[j].Username == username) {
          alreadyExists = true
          break
        }
      }
    }
    
    twitchListener := CreateListener(username, "", i)
    twitchListener.ChannelBuffer.Push(user)
    i.ListenerPool = append(i.ListenerPool, *twitchListener)
    i.ConnectQueue.Push(twitchListener.Username)
  }
}

func (i *IRCDriver) PartChannel(name string) {
  
  for ind := 0; ind < len(i.ListenerPool); ind++ {
    
    i.ListenerPool[ind].Connection.Part("#" + name)
    delete(i.ListenerPool[ind].Channels, name)
  }
}

func (i *IRCDriver) ActiveChatter(data map[string]string) {
  
  defer func() {
    
    if r := recover(); r != nil {
      
      fmt.Println("--------")
      fmt.Println("PANIC!!!")
      fmt.Println("--------")
      fmt.Println(r)
      fmt.Println(data)
    }
  }()
  
  theUser := ogdm.IdentitySlim{
    Platform: "twitch",
    Display: data["display-name"],
    Login: strings.ToLower(data["display-name"]),
    PlatformID: data["user-id"] }
  
  for ind := 0; ind < len(i.ActiveChatters); ind++ {
    
    if (i.ActiveChatters[ind].Channel.PlatformID == data["room-id"]) {
      
      for j := 0; j < len(i.ActiveChatters[ind].Chatters); j++ {
        
        if (i.ActiveChatters[ind].Chatters[j].PlatformID == theUser.PlatformID) {
          
          return
        }
      }
      
      i.ActiveChatters[ind].Chatters = append(i.ActiveChatters[ind].Chatters, theUser)
      
      return
    }
  }
  
  i.ActiveChatters = append(i.ActiveChatters, ogdm.ChattersBatch{
    Channel: ogdm.IdentitySlim{
      PlatformID: data["room-id"],
      Login: data["channel"],
      Platform: "twitch",
      },
      Chatters: []ogdm.IdentitySlim{ theUser },
    })
}

func (i *IRCDriver) FireEvent(e *ogdm.Event) {
  
  if (i.IsPrimary) { i.BufferEvents.Push(e) }
}

func (i *IRCDriver) CloseListeners() {
  
  for ind := 0; ind < len(i.ListenerPool); ind++ {
    
    i.ListenerPool[ind].Connection.Quit()
  }
  
  i.ConnectTicker.Stop()
  i.ChattersTicker.Stop()
  i.EventClient.Close()
  i.ChatClient.Close()
}

// Specifies an IRC Listener data structure.
type Listener struct {
  
  Username      string
  Listening     bool
  ChannelBuffer *ogdm.IdentityQueue
  Connection    *irc.Connection
  IrcDriver     *IRCDriver
  Channels      map[string]*ogdm.IdentitySlim
  Disconnected  bool
  RetryLater    bool
}

// Static. Creates a Listener using the specified nickname, and password.
func CreateListener (nick, token string, d *IRCDriver) *Listener {
  conn := irc.IRC(strings.ToLower(nick), nick)
  l := Listener{
    Username: nick,
    Listening: false,
    ChannelBuffer: ogdm.IdentityQueueNew(10000),
    Connection: conn,
    IrcDriver: d,
    Channels: make(map[string]*ogdm.IdentitySlim, config.ChannelsPerListener),
    Disconnected: false,
    RetryLater: false }
  
  l.Connection.Password = "oauth:" + token
  l.Connection.UseTLS = true
  l.Connection.TLSConfig = &tls.Config{}
  l.Connection.AddCallback("001", l.On001)
  l.Connection.AddCallback("CAP", l.OnCapAck)
  l.Connection.AddCallback("NOTICE", l.OnNotice)
  l.Connection.AddCallback("USERNOTICE", l.OnUserNotice)
  l.Connection.AddCallback("PRIVMSG", l.OnMessage)
  l.Connection.AddCallback("RECONNECT", func(e *irc.Event) { fmt.Println("Twitch issued reconnect.") })
  
  fmt.Println("Created listener \"" + nick + "\".")
  
  return &l;
}

// Listener. Begins Listening to the Twitch IRC servers.
func (l *Listener) Listen() {
  
  l.Listening = true
  
  if err := l.Connection.Connect("irc.chat.twitch.tv:443"); err != nil {
    fmt.Printf("Connection error: %s\n", err.Error())
  }
  
  ticker := time.NewTicker(50 * time.Millisecond)
  go func() {
    for {
      select {
        case <- ticker.C:
        if (!l.Disconnected) {
          nextChannel := l.ChannelBuffer.Pop()
          if (nextChannel != nil) {
            l.Connection.Join("#" + nextChannel.Login)
            l.Channels[nextChannel.Login] = nextChannel
          }
        }
      }
    }
  }()
  
  go l.Connection.Loop()
  
  go func() {
    
    err := <- l.Connection.Error
    fmt.Println("Twich IRC error: " + err.Error())
    l.Disconnected = true
    if (l.RetryLater) { time.Sleep(time.Second) }
    closer <- true
  }()
}

// Listener. Called when the client connects to the IRC server.
func (l *Listener) On001(e *irc.Event) {
  
  l.Connection.SendRaw("CAP REQ :twitch.tv/commands")
  l.Connection.SendRaw("CAP REQ :twitch.tv/tags")
  
  if (l.Disconnected) {
    
    for _, channel := range l.Channels {
      
      l.ChannelBuffer.Push(channel)
    }
    
    l.Disconnected = false
  }
}

// Listener. Called when the IRC server acknowledges a capability the client requests.
func (l *Listener) OnCapAck(e *irc.Event) {
  
  fmt.Println("Twitch acknowledged capability: ", strings.Split(e.Raw, " :")[1])
}

// Listener. Called when the IRC server issues a USERNOTICE message.
func (l *Listener) OnNotice(e *irc.Event) {
  
  defer func() {
    
    if r := recover(); r != nil {
      
      if (e.Message() == "Error logging in") {
        
        l.RetryLater = true
        return
      }
      
      fmt.Println("--------")
      fmt.Println("PANIC!!!")
      fmt.Println("--------")
      fmt.Println(r)
      fmt.Println("")
      fmt.Println(e.Raw)
    }
  }()
  
  data := ParseMessage(e.Raw)
  
  switch(data["msg-id"]) {
    
    case "host_on":
    event := CreateHostEvent(data, l.Channels)
    l.IrcDriver.FireEvent(event)
    case "host_off":
    event := CreateHostEvent(data, l.Channels)
    l.IrcDriver.FireEvent(event)
  }
}

// Listener. Called when the IRC server issues a USERNOTICE message.
func (l *Listener) OnUserNotice(e *irc.Event) {
  
  data := ParseMessage(e.Raw)
  
  if _, hasMsg := data["message"]; hasMsg {
    
    if (l.IrcDriver.IsPrimary) {
      
      l.IrcDriver.BufferChat.Push(e.Raw)
    }
  }
  
  switch(data["msg-id"]) {
    
    case "sub":
    event := CreateSubEvent(data)
    l.IrcDriver.FireEvent(event)
    case "resub":
    event := CreateSubEvent(data)
    l.IrcDriver.FireEvent(event)
    case "subgift":
    event := CreateSubEvent(data)
    l.IrcDriver.FireEvent(event)
    case "raid":
    event := CreateRaidEvent(data)
    l.IrcDriver.FireEvent(event)
    case "ritual":
    event := CreateRitualEvent(data)
    l.IrcDriver.FireEvent(event)
  }
}

// Listener. Called when the IRC server issues a PRIVMSG message.
func (l *Listener) OnMessage(e *irc.Event) {
  
  data := ParseMessage(e.Raw)

  if (data["username"] == "nifty255" &&
      data["message"] == "!bonk") {
    then, _ := strconv.ParseInt(data["tmi-sent-ts"], 10, 64)
    now := ogcl.ToUtcMilliseconds(time.Now())
    fmt.Println(data["tmi-sent-ts"], now, now - then)
  }
  
  if (l.IrcDriver.IsPrimary) {

    l.IrcDriver.BufferChat.Push(e.Raw)
  }
  
  bitsStr, bP := data["bits"]
  bitsNum, err := strconv.Atoi(bitsStr)
  
  cheermoteFinder := regexp.MustCompile(`^[A-Za-z]{3,15}\d+$`)
  
  words := strings.Split(data["message"], " ")
  cheermotes := make([]string, 0, len(words))
  for i := 0; i < len(words); i++ {
    
    if (cheermoteFinder.Match([]byte(words[i]))) {
      
      cheermotes = append(cheermotes, words[i])
    }
  }
  
  go l.IrcDriver.ActiveChatter(data)
  
  if (!bP || err != nil) { return; }
  
  event := ogdm.Event{
    Time: time.Now(),
    Platform: "twitch",
    EventID: data["id"],
    EventType: "bits",
    EventCmotes: cheermotes,
    EventAmount: bitsNum,
    EventMessage: data["message"],
    EventSenderID: data["user-id"],
    EventSenderLogin: data["username"],
    EventSenderDisplay: data["display-name"],
    EventTargetID: "",
    EventTargetLogin: "",
    EventTargetDisplay: "",
    EventChannelID: data["room-id"],
    EventChannelName: data["channel"] }
  
  l.IrcDriver.FireEvent(&event)
}

// Creates a host on/off Event using the provided map.
func CreateHostEvent(data map[string]string, users map[string]*ogdm.IdentitySlim) *ogdm.Event {
  
  timeOfEvent := time.Now()
  
  channelName := data["channel"]
  
  hostType := data["msg-id"]
  
  hostSenderId := ""
  hostSenderDisplay := ""
  hostSender, exists := users[channelName]
  if (exists) {
    hostSenderId = hostSender.PlatformID
    hostSenderDisplay = hostSender.Display
  }
  
  hostTarget := ""
  
  if (data["msg-id"] == "host_on") {
    
    msg := data["message"]
    hostTarget = ogcl.Substring(msg, 12, len(msg) - 13)
  }
  
  return &(ogdm.Event{
    Time: timeOfEvent,
    Platform: "twitch",
    EventID: "",
    EventType: hostType,
    EventSubtype: "",
    EventSenderID: hostSenderId,
    EventSenderLogin: channelName,
    EventSenderDisplay: hostSenderDisplay,
    EventTargetID: "",
    EventTargetLogin: strings.ToLower(hostTarget),
    EventTargetDisplay: hostTarget,
    EventChannelID: "",
    EventChannelName: "",
    EventAmount: 0,
    EventMessage: "",
    EventCmotes: []string{} })
}

// Creates a subscriber Event using the provided map.
func CreateSubEvent(data map[string]string) *ogdm.Event {
  
  timeOfEvent := time.Now()
  subId := data["id"]
  subType := data["msg-id"]
  subTier := data["msg-param-sub-plan"]
  subSenderId := data["user-id"]
  subSenderLogin := data["login"]
  subSenderDisplay := data["display-name"]
  subTargetId, sTP := data["msg-param-recipient-id"]
  subTargetLogin, _ := data["msg-param-recipient-user-name"]
  subTargetDisplay, _ := data["msg-param-recipient-display-name"]
  subChannelId := data["room-id"]
  subChannelName := data["channel"]
  subMessage, mP := data["message"]
  subLengthStr := data["msg-param-months"]
  
  subLengthNum, err := strconv.Atoi(subLengthStr)
  
  if (err != nil) {
    
    subLengthNum = -1
  }
  
  if (subLengthNum == 0) { subLengthNum++ }
  
  if (!sTP) {
    
    subTargetId = subSenderId
    subTargetLogin = subSenderLogin
    subTargetDisplay = subSenderDisplay
    
    subSenderId = ""
    subSenderLogin = ""
    subSenderDisplay = ""
  }
  
  if (!mP) { subMessage = "" }
  
  return &(ogdm.Event{
    Time: timeOfEvent,
    Platform: "twitch",
    EventID: subId,
    EventType: subType,
    EventSubtype: subTier,
    EventSenderID: subSenderId,
    EventSenderLogin: subSenderLogin,
    EventSenderDisplay: subSenderDisplay,
    EventTargetID: subTargetId,
    EventTargetLogin: subTargetLogin,
    EventTargetDisplay: subTargetDisplay,
    EventChannelID: subChannelId,
    EventChannelName: subChannelName,
    EventAmount: subLengthNum,
    EventMessage: subMessage,
    EventCmotes: []string{} })
}

// Creates a raid Event using the provided map.
func CreateRaidEvent(data map[string]string) *ogdm.Event {
  
  timeOfEvent := time.Now()
  raidId := data["id"]
  raidType := data["msg-id"]
  raidAmountStr := data["msg-param-viewerCount"]
  raidSenderId := data["user-id"]
  raidSenderLogin := data["login"]
  raidSenderDisplay := data["display-name"]
  raidChannelId := data["room-id"]
  raidChannelName := data["channel"]
  
  raidAmountNum, err := strconv.Atoi(raidAmountStr)
  
  if (err != nil) {
    
    raidAmountNum = -1
  }
  
  return &(ogdm.Event{
    Time: timeOfEvent,
    Platform: "twitch",
    EventID: raidId,
    EventType: raidType,
    EventSubtype: "",
    EventSenderID: raidSenderId,
    EventSenderLogin: raidSenderLogin,
    EventSenderDisplay: raidSenderDisplay,
    EventTargetID: raidChannelId,
    EventTargetLogin: raidChannelName,
    EventTargetDisplay: "",
    EventChannelID: "",
    EventChannelName: "",
    EventAmount: raidAmountNum,
    EventMessage: "",
    EventCmotes: []string{} })
}

// Creates a ritual Event using the provided map.
func CreateRitualEvent(data map[string]string) *ogdm.Event {
  
  timeOfEvent := time.Now()
  ritualId := data["id"]
  ritualType := data["msg-id"]
  ritualName := data["msg-param-ritual-name"]
  ritualSenderId := data["user-id"]
  ritualSenderLogin := data["login"]
  ritualSenderDisplay := data["display-name"]
  ritualChannelId := data["room-id"]
  ritualChannelName := data["channel"]
  
  return &(ogdm.Event{
    Time: timeOfEvent,
    Platform: "twitch",
    EventID: ritualId,
    EventType: ritualType,
    EventSubtype: ritualName,
    EventSenderID: ritualSenderId,
    EventSenderLogin: ritualSenderLogin,
    EventSenderDisplay: ritualSenderDisplay,
    EventTargetID: "",
    EventTargetLogin: "",
    EventTargetDisplay: "",
    EventChannelID: ritualChannelId,
    EventChannelName: ritualChannelName,
    EventAmount: -1,
    EventMessage: "",
    EventCmotes: []string{} })
}

// Parses a raw string message into a map.
func ParseMessage(raw string) map[string]string {
  
  m := make(map[string]string)
  theSplit := strings.Split(raw, " :")
  pairs := strings.Split(theSplit[0], ";")
  m["username"] = strings.Split(theSplit[1], "!")[0]
  m["channel"] = strings.Split(theSplit[1], "#")[1]
  if (len(theSplit) == 3) { m["message"] = theSplit[2] }
  
  for i := 0; i < len(pairs); i++ {
    p := strings.Split(pairs[i], "=")
    if (len(p) == 2) { m[p[0]] = p[1] }
  }
  return m
}
