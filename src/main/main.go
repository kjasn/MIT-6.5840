package main

import (
	"fmt"
)


func add(i *int) {
	*i++
}

func request(i *int, flag *bool) {
	<-ch
	add(i)
	fmt.Printf(">>> start a request: %d\n", *i)
	if *i > 5 {
		*flag = true
	}
}

var ch = make(chan int, 5)

type A struct {
	a int
}

func main() {
	v := &A{3}
	v.a = 6 
	// (*v).a = 5
	fmt.Println((*v).a)
	
	// for i := 0; i < 5; i++ {
	// 	ch <- i
	// }

	// idx := 0
	// flag := false
	// for {
	// 	switch flag {
	// 	case false:
	// 		go request(&idx, &flag)
	// 		for !flag {
	// 		}
	// 		fallthrough
	// 	case true:
	// 		fmt.Println("over")
	// 		os.Exit(0)
	// 	}
	// }
}