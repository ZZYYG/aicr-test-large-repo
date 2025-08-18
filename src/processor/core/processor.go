package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// 处理器配置
type ProcessorConfig struct {
	MaxWorkers      int
	QueueSize       int
	ProcessTimeout  time.Duration
	RetryCount      int
	RetryDelay      time.Duration
	ShutdownTimeout time.Duration
}

// 默认配置
var DefaultConfig = ProcessorConfig{
	MaxWorkers:      10,
	QueueSize:       100,
	ProcessTimeout:  30 * time.Second,
	RetryCount:      3,
	RetryDelay:      5 * time.Second,
	ShutdownTimeout: 60 * time.Second,
}

// 任务接口
type Task interface {
	Process(ctx context.Context) (interface{}, error)
	ID() string
	Priority() int
}

// 任务结果
type TaskResult struct {
	TaskID    string
	Result    interface{}
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Attempts  int
}

// 处理器
type Processor struct {
	config     ProcessorConfig
	tasks      chan Task
	results    chan TaskResult
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	mu         sync.Mutex
	isRunning  bool
}

// 创建新处理器
func NewProcessor(config ProcessorConfig) *Processor {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Processor{
		config:     config,
		tasks:      make(chan Task, config.QueueSize),
		results:    make(chan TaskResult, config.QueueSize),
		ctx:        ctx,
		cancelFunc: cancel,
		isRunning:  false,
	}
}

// 启动处理器
func (p *Processor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.isRunning {
		return errors.New("处理器已经在运行")
	}
	
	p.isRunning = true
	
	// 启动工作协程
	for i := 0; i < p.config.MaxWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	
	return nil
}

// 停止处理器
func (p *Processor) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if !p.isRunning {
		return errors.New("处理器未运行")
	}
	
	// 取消上下文
	p.cancelFunc()
	
	// 等待所有工作协程完成
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	
	// 等待工作协程完成或超时
	select {
	case <-done:
		// 所有工作协程已完成
	case <-time.After(p.config.ShutdownTimeout):
		return errors.New("处理器停止超时")
	}
	
	p.isRunning = false
	return nil
}

// 提交任务
func (p *Processor) SubmitTask(task Task) error {
	if !p.isRunning {
		return errors.New("处理器未运行")
	}
	
	select {
	case p.tasks <- task:
		return nil
	case <-p.ctx.Done():
		return errors.New("处理器已停止")
	default:
		return errors.New("任务队列已满")
	}
}

// 获取结果
func (p *Processor) GetResult() (TaskResult, error) {
	select {
	case result := <-p.results:
		return result, nil
	case <-p.ctx.Done():
		return TaskResult{}, errors.New("处理器已停止")
	}
}

// 工作协程
func (p *Processor) worker(id int) {
	defer p.wg.Done()
	
	for {
		select {
		case <-p.ctx.Done():
			// 处理器已停止
			return
		case task := <-p.tasks:
			// 处理任务
			result := p.processTask(task)
			
			// 发送结果
			select {
			case p.results <- result:
				// 结果已发送
			case <-p.ctx.Done():
				// 处理器已停止
				return
			}
		}
	}
}

// 处理任务
func (p *Processor) processTask(task Task) TaskResult {
	result := TaskResult{
		TaskID:    task.ID(),
		StartTime: time.Now(),
		Attempts:  0,
	}
	
	var err error
	var taskResult interface{}
	
	// 重试逻辑
	for attempt := 0; attempt < p.config.RetryCount; attempt++ {
		result.Attempts++
		
		// 创建带超时的上下文
		ctx, cancel := context.WithTimeout(p.ctx, p.config.ProcessTimeout)
		
		// 处理任务
		taskResult, err = task.Process(ctx)
		
		// 取消上下文
		cancel()
		
		// 如果成功或上下文已取消，则退出重试循环
		if err == nil || errors.Is(err, context.Canceled) {
			break
		}
		
		// 如果是超时错误，则继续重试
		if errors.Is(err, context.DeadlineExceeded) {
			// 等待重试延迟
			select {
			case <-time.After(p.config.RetryDelay):
				// 继续重试
			case <-p.ctx.Done():
				// 处理器已停止
				err = fmt.Errorf("处理器已停止: %w", p.ctx.Err())
				break
			}
		} else {
			// 其他错误，不重试
			break
		}
	}
	
	result.Result = taskResult
	result.Error = err
	result.EndTime = time.Now()
	
	return result
}

// 获取处理器状态
func (p *Processor) Status() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	return map[string]interface{}{
		"is_running":       p.isRunning,
		"max_workers":      p.config.MaxWorkers,
		"queue_size":       p.config.QueueSize,
		"process_timeout":  p.config.ProcessTimeout.String(),
		"retry_count":      p.config.RetryCount,
		"retry_delay":      p.config.RetryDelay.String(),
		"shutdown_timeout": p.config.ShutdownTimeout.String(),
	}
}
