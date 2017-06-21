package akka.testkit

import java.util.concurrent.ThreadFactory

import akka.actor.{ ActorSystem, Cancellable, Scheduler }
import akka.event.LoggingAdapter
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
 * For testing: scheduler that does not look at the clock, but must be progressed manually by calling `timePasses`.
 *
 * This is not entirely realistic: jobs will be executed on the test thread instead of using the `ExecutionContext`, but does
 * allow for faster and less timing-sensitive specs..
 */
class ExplicitlyTriggeredScheduler(config: Config, log: LoggingAdapter, tf: ThreadFactory) extends Scheduler {

  case class Item(time: Long, interval: Option[FiniteDuration], runnable: Runnable)

  var currentTime = 0L
  var scheduled: List[Item] = Nil

  override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable =
    schedule(initialDelay, Some(interval), runnable)

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable =
    schedule(delay, None, runnable)

  def timePasses(amount: FiniteDuration)(implicit system: ActorSystem) = {
    executeTasks(currentTime + amount.dilated.toMillis)
    currentTime += amount.dilated.toMillis
  }

  @tailrec
  private def executeTasks(runTo: Long): Unit = {
    scheduled
      .filter(_.time <= runTo)
      .sortBy(_.time)
      .headOption match {
        case Some(task) ⇒
          task.runnable.run()
          scheduled = scheduled.filter(_ != task) ++ task.interval.map(v ⇒ task.copy(time = task.time + v.toMillis))

          // running the runnable might have scheduled new events
          executeTasks(runTo)
        case _ ⇒ // Done
      }
  }

  private def schedule(initialDelay: FiniteDuration, interval: Option[FiniteDuration], runnable: Runnable)(implicit executor: ExecutionContext): Cancellable = {
    val item = Item(currentTime + initialDelay.toMillis, interval, runnable)
    scheduled = item +: scheduled

    if (initialDelay == Duration.Zero)
      executeTasks(currentTime)

    new Cancellable {
      var cancelled = false

      override def cancel(): Boolean = {
        val before = scheduled.size
        scheduled = scheduled.filter(_ != item)
        cancelled = true
        before > scheduled.size
      }

      override def isCancelled: Boolean = cancelled
    }
  }

  override def maxFrequency: Double = 42
}
