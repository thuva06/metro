# Metro

The phone app of Metro, a TV launcher, for use away from home. It reaches the TV and the AC hub through a public MQTT
broker; every message is sealed (AES-256-GCM) with a key only the paired devices hold, so the broker carries nothing it
can read. Nothing here works without that key.
