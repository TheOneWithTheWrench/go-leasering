package leasering

// simulateCrash stops background workers without cleaning up leases (for testing).
func (r *Ring) simulateCrash() {
	if r.coordinator != nil && r.coordinator.cancel != nil {
		r.coordinator.cancel()
	}
}
