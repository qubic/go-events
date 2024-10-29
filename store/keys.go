package store

import "encoding/binary"

const (
	TickEventsCollection      = 0x00
	TickProcessTimeCollection = 0x01
)

const (
	LastProcessedTick         = 0x01
	SkippedTicksInterval      = 0x02
	ProcessedTickIntervals    = 0x03
	LastProcessedTickPerEpoch = 0x04
)

func tickEventsKey(tickNumber uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{TickEvents}, tickNumber)
}

func lastProcessedTickKey() []byte {
	return []byte{LastProcessedTick}
}

func lastProcessedTickKeyPerEpoch(epochNumber uint32) []byte {
	key := []byte{LastProcessedTickPerEpoch}
	key = binary.BigEndian.AppendUint32(key, epochNumber)

	return key
}

func skippedTicksIntervalKey() []byte {
	return []byte{SkippedTicksInterval}
}

func processedTickIntervalsPerEpochKey(epoch uint32) []byte {
	key := []byte{ProcessedTickIntervals}
	key = binary.BigEndian.AppendUint32(key, epoch)

	return key
}

func tickProcessTimeKey(tickNumber uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{TickProcessTime}, tickNumber)
}
