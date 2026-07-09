using System;

namespace Natify
{
    public class Config
    {
        public int MaxCount = 1000;
        public int MaxSize = 50 * 1024; // 50 KB
        public TimeSpan MaxWait = TimeSpan.FromMilliseconds(50);
    }
}