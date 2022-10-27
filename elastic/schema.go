package elastic

// EsMappings contains the elasticsearch mappings
var EsMappings = map[string]string{
	"tx": `{
		"properties":{
			"ts": {
				"type": "date"
			},
			"blockno": {
				"type": "long"
			},
			"from": {
				"type": "keyword"
			},
			"to": {
				"type": "keyword"
			},
			"amount": {
				"enabled": false
			},
			"amount_float": {
				"type": "float"
			},
			"type": {
				"type": "keyword"
			},
			"category": {
				"type": "keyword"
			},
			"method": {
				"type": "keyword"
			},
			"token_transfers": {
				"type": "long"
			}
		}
	}`,
	"block": `{
		"properties": {
			"ts": {
				"type": "date"
			},
			"no": {
				"type": "long"
			},
			"txs": {
				"type": "long"
			},
			"size": {
				"type": "long"
			},
			"reward_account": {
				"type": "keyword"
			},
			"reward_amount": {
				"enabled": false
			}
		}
	}`,
	"name": `{
		"properties": {
			"name": {
				"type": "keyword"
			},
			"address": {
				"type": "keyword"
			},
			"blockno": {
				"type": "long"
			},
			"tx": {
				"type": "keyword"
			}
		}
	}`,
	"token_transfer": `{
		"properties":{
			"tx_id": {
				"type": "keyword"
			},
			"blockno": {
				"type": "long"
			},
			"ts": {
				"type": "date"
			},
			"address": {
				"type": "keyword"
			},
			"token_id": {
				"type": "keyword"
			},
			"from": {
				"type": "keyword"
			},
			"to": {
				"type": "keyword"
			},
			"amount": {
				"enabled": false
			},
			"amount_float": {
				"type": "float"
			}
		}
	}`,
	"token": `{
		"properties":{
			"tx_id": {
				"type": "keyword"
			},
			"blockno": {
				"type": "long"
			},
			"name": {
				"type": "keyword"
			},
			"symbol": {
				"type": "keyword"
			},
			"decimals": {
				"type": "short"
			},
			"supply": {
				"enabled": false
			}
		}
	}`,
}
