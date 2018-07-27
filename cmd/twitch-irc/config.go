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
  "io/ioutil"
  "path/filepath"
  "encoding/json"
)

type ConfigAddresses struct {
  
  Event string  `json:"event"`
  Chat  string  `json:"chat"`
}

type ConfigPorts struct {
  
  Event int `json:"event"`
  Irc   int `json:"irc"`
  Chat  int `json:"chat"`
}

type ConfigDatabase struct {
  
  Urls    []string  `json:"urls"`
  Replset string    `json:"replset"`
  DbName  string    `json:"db_name"`
}

type Config struct {
  
  Name                string          `json:"name"`
  Version             string          `json:"version"`
  Addresses           ConfigAddresses `json:"addresses"`
  Ports               ConfigPorts     `json:"ports"`
  Database            ConfigDatabase  `json:"database"`
  ChannelsPerListener int             `json:"channels_per_listener"`
}

func LoadConfig(filename string) *Config {
  
  var config Config
  
  if (filename == "") { fmt.Println("File name empty."); return DefaultConfig() }
  
  // Load config file.
  configPath := filepath.FromSlash("bin/config/twitch-irc/" + filename + ".json")
  configData, err := ioutil.ReadFile(configPath)
  
  // If there was an error loading the config file, or there was an error parsing it, load defaults.
  if (err != nil) {
    
    // Error loading file.
    fmt.Println("No config file found at \"" + configPath + "\".")
    return DefaultConfig()
  } else {
    
    // Parse the config file.
    err2 := json.Unmarshal(configData, &config)
    
    if (err2 != nil) {
      
      // Error parsing file.
      fmt.Println("Error parsing config file:", err2)
      return DefaultConfig()
    } else {
      
      return &config
    }
  }
}

func DefaultConfig() *Config {
  
  return &(Config{
    Name: "twitch-irc",
    Version: "1.0.0",
    Addresses: ConfigAddresses{
      Event: "127.0.0.1",
      Chat: "127.0.0.1" },
    Ports: ConfigPorts{
      Event: 3000,
      Irc: 3001,
      Chat: 3003 },
    Database: ConfigDatabase{
      Urls: []string{ "localhost:56789" },
      Replset: "",
      DbName: "opera_gather_template" },
    ChannelsPerListener: 1000 })
}