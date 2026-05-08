package retryqueue

import "errors"

var ErrIdRequired = errors.New("task id is required")
var ErrQueueClosed = errors.New("retry queue closed")
var ErrTaskNotFound = errors.New("task not found")
var ErrJsonUnMarshal = errors.New("json unmarshal error")
