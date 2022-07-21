<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class Consumer extends Worker
{
    /** @var Container */
    protected $container;

    /** @var string */
    protected $consumerTag;

    /** @var int */
    protected $prefetchSize;

    /** @var int */
    protected $maxPriority;

    /** @var int */
    protected $prefetchCount;

    /** @var bool  */
    protected $blocking = false;

    /** @var bool */
    protected $asyncSignalsSupported;

    /** @var AMQPChannel */
    protected $channel;

    /** @var bool */
    protected $gotJob = false;

    /**
     * @inheritDoc
     */
    public function __construct(QueueManager $manager, Dispatcher $events, ExceptionHandler $exceptions, callable $isDownForMaintenance)
    {
        parent::__construct($manager, $events, $exceptions, $isDownForMaintenance);
        $this->asyncSignalsSupported = $this->supportsAsyncSignals();
    }

    public function setContainer(Container $value): void
    {
        $this->container = $value;
    }

    public function setConsumerTag(string $value): void
    {
        $this->consumerTag = $value;
    }

    public function setMaxPriority(int $value): void
    {
        $this->maxPriority = $value;
    }

    public function setPrefetchSize(int $value): void
    {
        $this->prefetchSize = $value;
    }

    public function setPrefetchCount(int $value): void
    {
        $this->prefetchCount = $value;
    }

    public function setBlocking(bool $value): void
    {
        $this->blocking = $value;
    }

    /**
     * Listen to the given queue in a loop.
     *
     * @param string $connectionName
     * @param string $queue
     * @param WorkerOptions $options
     * @return int
     * @throws Throwable
     */
    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        if ($this->asyncSignalsSupported) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->connection($connectionName);

        $this->channel = $connection->getChannel();

        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            null
        );

        $jobClass = $connection->getJobClass();
        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        while (true) {
            if ($this->blocking && ! $this->daemonShouldRun($options, $connectionName, $queue)) {
                if ($this->asyncSignalsSupported) {
                    pcntl_signal_dispatch();
                }

                $this->pauseWorker($options, $lastRestart);

                continue;
            }

            $this->channel->basic_consume(
                $queue,
                $this->consumerTag,
                false,
                false,
                false,
                false,
                function (AMQPMessage $message) use ($connection, $options, $connectionName, $queue, $jobClass, &$jobsProcessed): void {
                    $job = new $jobClass(
                        $this->container,
                        $connection,
                        $message,
                        $connectionName,
                        $queue
                    );

                    if ($this->supportsAsyncSignals()) {
                        $this->registerTimeoutHandler($job, $options);
                    }

                    $this->runJob($job, $connectionName, $options);

                    if ($this->supportsAsyncSignals()) {
                        $this->resetTimeoutHandler();
                    }
                },
                null,
                $arguments
            );

            while ($this->channel->is_consuming()) {
                // Before reserving any jobs, we will make sure this queue is not paused and
                // if it is we will just pause this worker for a given amount of time and
                // make sure we do not need to kill this worker process off completely.
                if (! $this->blocking && ! $this->daemonShouldRun($options, $connectionName, $queue)) {
                    $this->pauseWorker($options, $lastRestart);

                    continue;
                }

                // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
                try {
                    $this->channel->wait(null, ! $this->blocking);
                } catch (AMQPRuntimeException $exception) {
                    $this->exceptions->report($exception);

                    $this->kill(1);
                } catch (Exception | Throwable $exception) {
                    $this->exceptions->report($exception);

                    $this->stopWorkerIfLostConnection($exception);
                }

                // If no job is got off the queue, we will need to sleep the worker.
                if (! $this->blocking && ! $this->gotJob) {
                    $this->sleep($options->sleep);
                }

                // Finally, we will check to see if we have exceeded our memory limits or if
                // the queue should restart based on other indications. If so, we'll stop
                // this worker and let whatever is "monitoring" it restart the process.
                $this->stopIfNecessary($options, $lastRestart, $this->gotJob ? true : null);

                $this->gotJob = false;
            }

            if (! $this->blocking) {
                break;
            }
        }
    }

    /**
     * Determine if the daemon should process on this iteration.
     *
     * @param WorkerOptions $options
     * @param string $connectionName
     * @param string $queue
     * @return bool
     */
    protected function daemonShouldRun(WorkerOptions $options, $connectionName, $queue): bool
    {
        return ! ((($this->isDownForMaintenance)() && ! $options->force) || $this->paused);
    }

    /**
     * Stop listening and bail out of the script.
     *
     * @param  int  $status
     * @return void
     */
    public function stop($status = 0): void
    {
        // Tell the server you are going to stop consuming.
        // It will finish up the last message and not send you any more.
        $this->channel->basic_cancel($this->consumerTag, false, true);

        parent::stop($status);
    }

    /**
     * @inheritDoc
     */
    protected function listenForSignals()
    {
        if (! $this->blocking) {
            parent::listenForSignals();
            return;
        }

        // Support pause/exit for blocking mode
        pcntl_async_signals(true);
        pcntl_signal(SIGHUP, function () {
            $this->shouldQuit = true;
            $this->channel->close();
        });
        pcntl_signal(SIGTERM, function () {
            $this->shouldQuit = true;
            $this->channel->close();
        });

        pcntl_signal(SIGUSR2, function () {
            $this->paused = true;
            $this->channel->basic_cancel($this->consumerTag);
        });

        pcntl_signal(SIGCONT, function () {
            $this->paused = false;
        });
    }
}
