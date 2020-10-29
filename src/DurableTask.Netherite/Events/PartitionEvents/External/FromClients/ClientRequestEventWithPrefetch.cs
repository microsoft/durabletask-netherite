//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;

    [DataContract]
abstract class ClientRequestEventWithPrefetch : ClientRequestEvent, IClientRequestEvent
    {
        [DataMember]
        public ProcessingPhase Phase { get; set; }

        [IgnoreDataMember]
        public abstract TrackedObjectKey Target { get; }

        public virtual bool OnReadComplete(TrackedObject target, Partition partition)
        {
            return true;
        }

        [IgnoreDataMember]
        public virtual TrackedObjectKey? Prefetch => null;

        public sealed override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Prefetch);
        }

        public enum ProcessingPhase
        { 
             Read,
             Confirm,
             ConfirmAndProcess,
        }
    }
}