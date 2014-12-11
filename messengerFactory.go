package GoMM

// Returns the ChannelMessengers and adds them all to the supplied resolver map.
func GetChannelMessengers(names []string, resolverMap map[string]chan Message) []ChannelMessenger {
	num := len(names)
	messengers := make([]ChannelMessenger, num)

	// Generate resolver map
	for i := 0; i < num; i++ {
		name := names[i]
		messengers[i] = GetChannelMessenger(name, resolverMap)
	}

	// Update the messengers resolver map
	for i := 0; i < num; i++ {
		messengers[i].ResolverMap = resolverMap
	}

	return messengers
}

func GetChannelMessenger(name string, resolverMap map[string]chan Message) ChannelMessenger {
	channel := make(chan Message)
	resolverMap[name] = channel
	messenger := ChannelMessenger{}
	messenger.Incoming = channel

	return messenger
}
