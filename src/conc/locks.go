package "goru/conc"

import (

	)

type Locker interface {
	Lock()
	Unlock()
}	

type PetersonLock struct {
	
}	
