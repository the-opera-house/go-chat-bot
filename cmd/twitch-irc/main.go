/*
*
* Name:     Twitch IRC Listener
* Sys Name: twitch-irc
* Author:   Nifty255
*
*/

package main

import (
  "fmt"
  "os"
  "os/signal"
  "syscall"
  "errors"
  "github.com/koding/kite"
  ogcn "github.com/the-opera-house/go-common-lib/net"
  ogdm "github.com/the-opera-house/go-common-lib/models"
)

var config    *Config
var ircDriver *IRCDriver
var closer    chan bool

func main() {
  
  closer = make(chan bool)
  
  envType := os.Getenv("ENVTYPE")
  
  config = LoadConfig(envType)
  
  k := kite.New(config.Name, config.Version)
  
  dbDriver := ogcn.DatabaseDriverNew(config.Database.Urls, config.Database.Replset, config.Database.DbName)
  ircDriver = CreateIrcDriver(dbDriver, k)
  
  k.HandleFunc("force-restart", Restart).DisableAuthentication()
  k.HandleFunc("set-primary", SetPrimary).DisableAuthentication()
  k.HandleFunc("are-you-primary", PrimaryCheck).DisableAuthentication()
  k.HandleFunc("are-you-ready", ReadyCheck).DisableAuthentication()
  k.HandleFunc("listen-to-channels", ListenToChannels).DisableAuthentication()
  
  k.Config.Port = config.Ports.Irc
  go k.Run()
  
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs)
  shouldQuit := false
  
  for shouldQuit == false {
    
    select {
      
      case s := <-sigs:
        fmt.Println("RECEIVED SIGNAL:", s)
        if (s == syscall.SIGTERM ||
            s == syscall.SIGKILL ||
            s == syscall.SIGQUIT ||
            s == syscall.SIGINT) {

          shouldQuit = true
        }
      case <- closer:
        fmt.Println("INTERNAL CLOSURE.")
        shouldQuit = true
    }
  }
  
  ircDriver.CloseListeners()
  
  k.Close()
  
  panic(errors.New("Closing!"))
}

func Restart(r *kite.Request) (interface{}, error) {
  
  closer <- true
  
  return true, nil
}

func SetPrimary(r *kite.Request) (interface{}, error) {
  
  isPrimary, err := r.Args.One().Bool()
  
  if (err != nil) {
    
    return ircDriver.IsPrimary, err
  }
  
  ircDriver.IsPrimary = isPrimary
  
  if (isPrimary) {
    fmt.Println("Made primary!")
  } else {
    fmt.Println("No longer primary")
  }
  
  return isPrimary, nil
}

func PrimaryCheck(r *kite.Request) (interface{}, error) {
  
  return ircDriver.IsPrimary, nil
}

func ReadyCheck(r *kite.Request) (interface{}, error) {
  
  for i := 0; i < len(ircDriver.ListenerPool); i++ {
    
    if (ircDriver.ListenerPool[i].ChannelBuffer.Count > 0) {
      
      return false, nil
    }
  }
  
  return true, nil
}

func ListenToChannels(r *kite.Request) (interface{}, error) {
  
  channels := make([]ogdm.IdentitySlim, 0, 10000)
  
  if err := r.Args.One().Unmarshal(&channels); err != nil {
    
    fmt.Println(err)
    return false, errors.New("Unable to unmarshal.")
  }
  
  fmt.Println("Told to listen to", len(channels), "channels.")
  
  if (len(channels) == 0) { return false, errors.New("Empty list.") }
  
  for i := 0; i < len(channels); i++ {
    
    ircDriver.ListenToChannel(&channels[i])
  }
  
  return true, nil
}
