from mesos.interface import Scheduler
from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver


class SimpleMesosScheduler(Scheduler):
    def __init__(self):
        self.tasks_launched = 0
        self.tasks_finished = 0

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            if self.tasks_launched < 5:
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(self.tasks_launched)
                task.slave_id.value = offer.slave_id.value
                task.name = "task {}".format(task.task_id.value)

                # Specify resources required for the task
                task_resources = task.resources.add()
                task_resources.name = "cpus"
                task_resources.type = mesos_pb2.Value.SCALAR
                task_resources.scalar.value = 1  # Request 1 CPU

                task_resources = task.resources.add()
                task_resources.name = "mem"
                task_resources.type = mesos_pb2.Value.SCALAR
                task_resources.scalar.value = 128  # Request 128 MB of memory

                command = mesos_pb2.CommandInfo()
                command.value = "echo Hello World"
                task.command.MergeFrom(command)

                tasks.append(task)
                self.tasks_launched += 1

            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, status):
        if status.state == mesos_pb2.TASK_FINISHED:
            self.tasks_finished += 1
            print("Task {} finished".format(status.task_id.value))

        if self.tasks_finished >= 5:
            driver.stop()

class RDD:
    def __init__(self, data):
        self.data = data
        self.transformations = []

    def map(self, func):
        self.transformations.append(('map', func))
        return self

    def filter(self, func):
        self.transformations.append(('filter', func))
        return self

    def collect(self):
        result = self.data
        for name, func in self.transformations:
            if name == 'map':
                result = map(func, result)
            elif name == 'filter':
                result = filter(func, result)
        return list(result)

    def count(self):
        return len(self.collect())

    def flatMap(self, func):
        self.transformations.append(('flatMap', func))
        return self

    def reduce(self, func):
        return reduce(func, self.collect())


class SparkContextWithMesos:
    def __init__(self, master):
        self.scheduler = SimpleMesosScheduler()
        self.driver = MesosSchedulerDriver(self.scheduler, mesos_pb2.FrameworkInfo(user="", name="Simple Framework"), master)

    def parallelize(self, data):
        return RDD(data)

    def start(self):
        self.driver.start()
        self.driver.join()

    def stop(self):
        self.driver.stop()

if __name__ == "__main__":
    sc = SparkContextWithMesos(master="mesos://localhost:5050")

    # Start the scheduler (this will block until all tasks are finished)
    sc.start()
