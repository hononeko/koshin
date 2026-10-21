rootProject.name = "koshin"

include(":core")
include(":adapters:nats")
include(":adapters:state-valkey")
include(":adapters:github-app")
include(":proxy")
include(":worker")
