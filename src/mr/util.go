package mr

func (c *Coordinator) generateID() int {
	ret := c.TaskID
	c.TaskID++
	return ret
}
