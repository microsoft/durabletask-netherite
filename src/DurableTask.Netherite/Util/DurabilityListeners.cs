// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// Provides a concurrency-safe mechanism for subscribing listeners that get notified after
    /// events are durably persisted to partition storage or to queues.
    /// </summary>
    struct DurabilityListeners
    {
        volatile object status;
        static readonly object MarkAsSuccessfullyCompleted = new object();

        public static void Register(Event evt, TransportAbstraction.IDurabilityListener listener)
        {
            
            // fast path: status is null, replace it with the listener
            if (Interlocked.CompareExchange(ref evt.DurabilityListeners.status, listener, null) == null)
            {
                return;
            }

            // slower path: there are some listener(s) already, or the event is acked already
            while (true)
            {
                var current = evt.DurabilityListeners.status;

                // if the current status indicates the ack has happened already, notify the listener
                // right now

                if (current == MarkAsSuccessfullyCompleted)
                {
                    listener.ConfirmDurable(evt);
                    return;
                }

                if (current is Exception e && listener is TransportAbstraction.IDurabilityOrExceptionListener exceptionListener)
                {
                    exceptionListener.ReportException(evt, e);
                    return;
                }

                // add the listener to the list of listeners

                List<TransportAbstraction.IDurabilityListener> list;

                if (current is TransportAbstraction.IDurabilityListener existing)
                {
                    list = new List<TransportAbstraction.IDurabilityListener>() { existing, listener };
                }
                else
                {
                    list = (List<TransportAbstraction.IDurabilityListener>) current;
                    list.Add(listener);
                }

                if (Interlocked.CompareExchange(ref evt.DurabilityListeners.status, list, current) == current)
                {
                    return;
                }     
            }
        }

        public static void ConfirmDurable(Event evt)
        {
            var listeners = Interlocked.Exchange(ref evt.DurabilityListeners.status, MarkAsSuccessfullyCompleted);

            if (listeners != null)
            {
                using (EventTraceContext.MakeContext(0L, evt.EventIdString))
                {
                    if (listeners is TransportAbstraction.IDurabilityListener listener)
                    {
                        listener.ConfirmDurable(evt);
                    }
                    else if (listeners is List<TransportAbstraction.IDurabilityListener> list)
                    {
                        foreach (var l in list)
                        {
                            l.ConfirmDurable(evt);
                        }
                    }
                }
            }       
        }

        public static void ReportException(Event evt, Exception e)
        {
            if (e == null)
            {
                throw new ArgumentNullException(nameof(e));
            }

            var listeners = Interlocked.Exchange(ref evt.DurabilityListeners.status, e);

            if (listeners != null)
            {
                if (listeners is TransportAbstraction.IDurabilityListener listener)
                {
                    listener.ConfirmDurable(evt);
                }
                else if (listeners is List<TransportAbstraction.IDurabilityListener> list)
                {
                    foreach (var l in list)
                    {
                        l.ConfirmDurable(evt);
                    }
                }
            }
        }

        public void Clear()
        {
            this.status = default;
        }
    }
}
