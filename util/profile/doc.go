// Copyright 2022 The CubeFS Authors.
// application memory data controller and metric exporter in localhost.
//
// With tag `noprofile` to avoid generating scripts.
//
// Note:
//   1. You can register http handler to profile multiplexer to control.
//   2. You may use environment to set variables, like `bind_addr`, `metric_exporter_labels`.
//   3. You cannot access the localhost service before profile initialized.
//
// Example:
//
// package main
//
// import (
//     "net/http"
//
//     // using default handler in profile defined
//     _ "github.com/cubefs/blobstore/util/profile"
//
//     // you can register handler to profile in other module
//     "github.com/cubefs/blobstore/util/profile"
// )
//
// func registerHandler() {
//     profile.HandleFunc("/myself/controller", func(http.ResponseWriter, *http.Request) {
//         // todo something
//     })
// }
//
// func main() {
//     registerHandler()
// }
package profile
