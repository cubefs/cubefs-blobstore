# 更新历史 


### [v0.9.2](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/compare/c0059422...97485de0) (2021-11-04)


### Chores | 其他更新

* **common:** remove common lib must ([15054a1](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/15054a1afbf6fc1ef8c1c8c1720f4f9780a9fbf1))


### Bug Fixes | Bug 修复

* **access:** tolerate one az was done when multi az write ([2070407](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/20704077f010917a30b617dccd05a700739da358)), closes [#92](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/issues/92)
* **allocator:** adjust code ([54890aa](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/54890aa5c2a964c0f448beb23611da0a52aca46d))
* **allocator:** allocator not alloc exclude vids ([613951a](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/613951a2c1e63d2ce8c8916d63345613fd2d4af7))
* **allocator:** assign current bid scope ([b2c860b](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/b2c860bb87803defd3c08270cb84bfc803bb8858))
* **allocator:** do not alloc excludes vid ([67a88e9](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/67a88e971c2498885991988126e66c94f0d1fafa))
* **allocator:** Don't choose punish vids ([aa46da9](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/aa46da93c39b354c15a9286b1319c8931be50f72))
* **clustermgr:** calculate writable space with blob node free size instead of free chunk ([bb2d3a0](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/bb2d3a0a75975454178d062d6a6bfd5abea4eeef))
* **tools:** fix auth curl issues ([16f4776](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/16f477679c8005701e5926021d301a6c1c54efd5))


### Code Refactoring | 代码重构

* **blobnode:**  Improve the function of ebsfio ([f8d6625](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/f8d6625d03ffa8a246778d646d31ae5dcc8343aa))
* **blobnode:**  Modify the configuration of the single machine, modify the configuration of iostat ([46b231b](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/46b231b1a3b8ad23bd7c3bef2c5fd00c9d66105f))
* **clustermgr:** calculate writable space optimized and add idc chunk and free chunk info on stat api and disk stat metric ([b240e02](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/b240e02f41ea6a13a0dfc8ad9f56f180bbb87561))
* **clustermgr:** check if satisfied with allocate policy after alloc chunks ([171ddea](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/171ddea407791a2a95d8dffbfae3db552584b8cc))
* **clustermgr:** modify import package sequence ([902c339](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/902c339b529214dd0d8b79313cab731ddb2526bc))


### Features | 新功能

* **blobnode:**  Refined delay tracking function ([97485de](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/97485de0ea641919d13a7b0968e8e9128a0d8867))
* **clustermgr:** add list volume v2 api to list specified status volume; add raft wal clear script ([13d9cdb](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/13d9cdb55060a4243088cac00588b1995d11867e))
* **clusterMgr:** add service list func ([964fba1](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/964fba18ffec522a6010d11d927e677d5bb862c9))
* **clustermgr:** alloc volume can not over disk load ([7a2a0e7](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/7a2a0e7a85c2d1b9ed80f72ecd9ffaa7e1631266))
* **clustermgr:** create volume with 2 step, optimize rubbish chunk problem when create volume failed ([dd9013f](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/dd9013fe01efce42b53fc268b47c4082595144d6)), closes [#89](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/issues/89)
* **tools:** auth curl tool ([4aa1cca](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/4aa1ccaa3d93752146cffb8637efd3bd6d8d61a3))
* **tools:** cm cmd command add secret config to support access cluster manager with auth token ([a0e1f3d](http://gitlab.oppoer.me/oppo_cloud_storage/ebs/commit/a0e1f3dd3717f1bc9fcb2c19f627299bc7bef783))
