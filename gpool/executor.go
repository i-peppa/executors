package gpool

import (
	"sync"
	"time"
	"github.com/satori/go.uuid"
	"git.wemomo.com/go/utils"
	"github.com/hashicorp/golang-lru"
	"bytes"
)

type executors struct {
	execChan chan executorChan
}


type executorChan struct {
	f func(...interface{})interface{}
	sw *sync.WaitGroup
	cacheStore *lru.Cache
	cacheKey string
}

/**
全局初始化
gonum 开启协程数
 */
func NewExecutors(gonum int) *executors {

	execChan := make(chan executorChan, gonum)

	for i := 0; i < gonum; i++ {
		go func() {
			for {
				func() {
					exec := <- execChan

					evicted := exec.cacheStore.Add(exec.cacheKey, exec.f())

					if evicted {
						utils.LogUtil.Info(exec.cacheKey)
					}

					defer exec.sw.Done()
				}()

			}
		}()
	}
	return &executors{
		execChan,
	}
}


//实际执行器

type executor struct {
	sw sync.WaitGroup
	ch chan executorChan
	cacaheStore *lru.Cache
}


//生成实际执行器
func (this *executors) NewExecutor() *executor {

	cacaheStore, error := lru.New(12500)

	if error != nil {
		utils.LogUtil.Error(error.Error())
		return nil
	}

	return &executor{
		sync.WaitGroup{},
		this.execChan,
		cacaheStore,
	}
}

//执行方法
func (this *executor) Execute(f func(...interface{}) interface{}, pre_loadkey string) string {

	uuid := uuid.NewV4()

	loadkey := pre_loadkey + ":" + uuid.String()

	this.sw.Add(1)

	this.cacaheStore.Add(loadkey, nil)

	this.ch <- executorChan{f, &this.sw, this.cacaheStore, loadkey}

	return loadkey

}

//获取数据
func (this *executor) Fetch(loadkey string) (interface{}, bool) {

	r, ok := this.cacaheStore.Peek(loadkey)

	this.cacaheStore.Remove(loadkey)

	if ok && r != nil {
		return r, true
	}

	return nil, false
}


func (this *executor) WaitTimeout(timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		this.sw.Wait()
	}()

	select {
	case <-c:
		return true // completed normally
	case <-time.After(timeout):

		var logkey bytes.Buffer

		for _, v := range this.cacaheStore.Keys() {
			_, error0 := logkey.WriteString("[")
			if error0 != nil {
				utils.LogUtil.Error(error0.Error())
			}
			_, error := logkey.WriteString(v.(string))
			if error != nil {
				utils.LogUtil.Error(error.Error())
			}
			_, error2 := logkey.WriteString("]")
			if error2 != nil {
				utils.LogUtil.Error(error2.Error())
			}
		}

		utils.LogUtil.Error(logkey.String() + " executor timeout")
		return false // timed out
	}

}