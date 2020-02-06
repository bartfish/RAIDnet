using System;
using System.Collections.Generic;
using System.Text;

namespace RAIDnet.HostModels
{
    public class DbDescription
    {
        public string Name { get; set; }

        public string Server { get; set; }

        public bool Exists { get; set; }

        public bool IsCurrentlyConnected { get; set; }

        public MirrorSide MirrorSide { get; set; }

        public CreationType ShouldBeRecreated { get; set; }

        public List<DbDescription> DbMirrors { get; set; } = new List<DbDescription>();

        public string ServerDirectory { get; internal set; }
    }
}
