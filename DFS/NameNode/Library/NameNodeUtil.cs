namespace BareDFS.NameNode.Library
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class NameNodeUtil
    {
        // utility method to validate a file path
        public static bool IsValidFilePath(string filePath)
        {
            return !string.IsNullOrEmpty(filePath) && filePath.StartsWith("/");
        }

        // utility method to format a file path
        public static string FormatFilePath(string filePath)
        {
            return filePath.Trim().Replace("\\", "/");
        }

        public static void LogMessage(string message)
        {
            Console.WriteLine($"[NameNode] {message}");
        }

        // Utility method to select random numbers from a list as many as specified by count
        public static List<Guid> SelectRandomNumbers(List<Guid> availableItems, ulong count, List<Guid> dataNodesToExclude = null)
        {
            var randomNumberSet = new HashSet<Guid>();
            var rand = new Random();

            while ((ulong)randomNumberSet.Count < count)
            {
                var chosenItem = availableItems[rand.Next(availableItems.Count)];
                if (dataNodesToExclude != null && dataNodesToExclude.Contains(chosenItem))
                    continue;
                randomNumberSet.Add(chosenItem);
            }

            return randomNumberSet.ToList();
        }

        public static List<Guid> AssignDataNodes(List<Guid> dataNodesAvailable, ulong replicationFactor, List<Guid> dataNodesToExclude = null)
        {
            var targetDataNodeIds = SelectRandomNumbers(dataNodesAvailable, replicationFactor, dataNodesToExclude);
            return targetDataNodeIds;
        }
    }
}