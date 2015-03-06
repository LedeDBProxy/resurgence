local config = {}

config.partitionLookupModule = "optivo.hscale.modulusPartitionLookup"
config.tableKeyColumns = {
    ["small"] = "category"
}
config.partitionsPerTable = 3

return config

