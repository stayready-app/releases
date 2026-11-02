package netreliable

import "example.com/srdt/secureudp"

func offeredCapabilities(o Options) secureudp.Capabilities {
	caps := secureudp.CapabilityControlPadding |
		secureudp.CapabilityStreaming |
		secureudp.CapabilityAdaptiveRTO |
		secureudp.CapabilityCongestionControl |
		secureudp.CapabilityIndependentStreams |
		secureudp.CapabilityPathLiveness
	if !o.LegacyPackets {
		caps |= secureudp.CapabilityPacketTracking
		if !o.LegacyPacketAcks {
			caps |= secureudp.CapabilityPacketAckBatch
		}
	}
	if !o.LegacyCredits {
		caps |= secureudp.CapabilityWindowCredits
	}
	return caps
}

func requiredCapabilities(o Options, extra secureudp.Capabilities) secureudp.Capabilities {
	caps := secureudp.CapabilityControlPadding | extra
	if !o.LegacyPackets {
		caps |= secureudp.CapabilityPacketTracking
	}
	if !o.LegacyCredits {
		caps |= secureudp.CapabilityWindowCredits
	}
	return caps
}

func serverSupportedCapabilities() secureudp.Capabilities {
	return secureudp.SupportedCapabilities
}

// The V1 server requires only authenticated control padding at the handshake
// layer. Packet tracking/window credits are required by the default client and
// can be explicitly disabled by compatibility/testing options without an
// implicit timeout downgrade.
func serverRequiredCapabilities() secureudp.Capabilities {
	return secureudp.CapabilityControlPadding
}
