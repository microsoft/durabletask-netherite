namespace DurableTask.Netherite.Darq
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using System.Threading.Channels;
    using System.Linq;

    class SystemEmulator
    {
        readonly Random random;

        readonly Dictionary<string, ComponentInfo> components = new Dictionary<string, ComponentInfo>();

        readonly SortedDictionary<(string, string), Queue<IEvent>> nonEmptyChannels = new SortedDictionary<(string, string), Queue<IEvent>>();
        readonly SortedDictionary<(string, long), Func<IEvent>> pendingTasks = new SortedDictionary<(string, long), Func<IEvent>>();

        bool nondeterministicContext = false; // used to check determinism constraint

        public SystemEmulator(int randomSeed = 0)
        {
            this.random = new Random(randomSeed);
        }

        public void AddComponent(Component component)
        {
            if (!this.components.TryAdd(component.ComponentId, new ComponentInfo(this, component)))
            {
                throw new InvalidOperationException("component must have unique id");
            }
        }

        class ComponentInfo : IContext
        {
            readonly SystemEmulator emulator;
            readonly Component component;
            readonly string componentId;

            public long HistoryPosition { get; private set; }

            public ComponentInfo(SystemEmulator emulator, Component component)
            {
                this.emulator = emulator;
                this.component = component;
            }

            public void SendSignal(IEvent evt, string destination)
            {
                if (this.emulator.nondeterministicContext) throw new InvalidOperationException("signals cannot be sent in nondeterministic context");
                this.HistoryPosition++;
                this.emulator.Enqueue(evt, this.component.ComponentId, destination);
            }

            public void RunTask(Func<IEvent> task)
            {
                if (this.emulator.nondeterministicContext) throw new InvalidOperationException("tasks cannot be run in nondeterministic context");
                var taskId = this.HistoryPosition++;
                this.emulator.pendingTasks.Add((this.component.ComponentId, taskId), task);
            }

            public void Deliver(IEvent message)
            {
                var pos = this.HistoryPosition++;
                this.component.Process(this, message, pos);
            }
        }

        public void Enqueue(IEvent evt, string source, string destination)
        {
            if (!this.nonEmptyChannels.TryGetValue((source, destination), out var queue))
            {
                this.nonEmptyChannels[(source, destination)] = queue = new Queue<IEvent>();
            }
            queue.Enqueue(evt);
        }

        public bool ProcessNext()
        {
            int numChoices = this.nonEmptyChannels.Count + this.pendingTasks.Count;

            if (numChoices > 0)
            {               
                int choice = this.random.Next(numChoices);

                if (choice < this.nonEmptyChannels.Count)
                {
                    var kvp = this.nonEmptyChannels.ElementAt(choice);
                    (string source, string destination) = kvp.Key;
                    var queue = kvp.Value;
                    this.components[destination].Deliver(queue.Dequeue());
                    if (queue.Count == 0)
                    {
                        this.nonEmptyChannels.Remove((source, destination));
                    }
                }
                else
                {
                    choice -= this.nonEmptyChannels.Count;
                    var kvp = this.pendingTasks.ElementAt(choice);
                    (string componentId, long taskId) = kvp.Key;
                    this.nondeterministicContext = true;
                    var result = kvp.Value();
                    this.nondeterministicContext = false;
                    this.Enqueue(result, componentId, componentId);
                }
            }

            return this.nonEmptyChannels.Count + this.pendingTasks.Count > 0;
        }
    }
}
