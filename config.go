package broadcast

/*

# Global redis config (priority - 2)
default:
   # redis configuration here

websockets: # <----- one of possible subscribers
  path: /ws
  broker: default # <------ broadcast broker to use --------------- |
                                                                    |  match
broadcast: # <-------- broadcast entry point plugin                 |
  default: # <----------------------------------------------------- |
     driver: redis
     # local redis config (priority - 1)
  test:
     driver: memory


priority local -> global
*/

// Config ...
type Config struct {
	Data map[string]any `mapstructure:"broadcast"`
}
