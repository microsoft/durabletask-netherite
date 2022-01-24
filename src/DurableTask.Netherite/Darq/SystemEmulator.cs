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
        readonly Dictionary<string, ComponentInfo> components = new Dictionary<string, ComponentInfo>();
        readonly Dictionary<(string, string), Queue<IEvent>> nonEmptyChannels = new Dictionary<(string, string), Queue<IEvent>>();
        readonly Random random;

        public SystemEmulator(int randomSeed = 0)
        {
            this.random = new Random(randomSeed);
        }

        class ComponentInfo : IContext
        {
            readonly SystemEmulator emulator;
            readonly Component component;
            readonly string componentId;

            public long Position;

            public ComponentInfo(SystemEmulator emulator, Component component)
            {
                this.emulator = emulator;
                this.component = component;
            }

            void IContext.SignalAsync(IEvent evt, string destination)
            {
                this.emulator.Enqueue(evt, this.component.ComponentId, destination);
            }

            void IContext.SignalSelfAsync(IEvent evt)
            {
                this.emulator.Enqueue(evt, this.component.ComponentId, this.component.ComponentId);
            }

            public void Deliver(IEvent message)
            {
                this.component.Process(this, message, this.Position++);
            }
        }

        public void AddComponent(Component component)
        {
            if (!this.components.TryAdd(component.ComponentId, new ComponentInfo(this, component)))
            {
                throw new InvalidOperationException("component must have unique id");
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
            if (this.nonEmptyChannels.Count == 0)
            {
                return false;
            }
            else
            {
                int choice = this.random.Next(this.nonEmptyChannels.Count);
                var kvp = this.nonEmptyChannels.ElementAt(choice);
                (string source, string destination) = kvp.Key;
                var queue = kvp.Value;
                this.components[destination].Deliver(queue.Dequeue());
                if (queue.Count == 0)
                {
                    this.nonEmptyChannels.Remove((source, destination));
                }
            }
        }
    }
}
