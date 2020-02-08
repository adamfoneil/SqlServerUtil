using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using SqlIntegration.Library.Extensions;
using System.Linq;

namespace Testing
{
    [TestClass]
    public class Extensions
    {
        [TestMethod]
        public void SimpleSegmentsExample()
        {
            var items = new int[]
            {
                1, 2, 3, 4, 5, 6, 7, 8, 9
            };

            var chunked = items.ToSegments(3);
            Assert.IsTrue(chunked.Count() == 3);
            Assert.IsTrue(chunked.All(c => c.Count() == 3));

            var array = chunked.ToArray();
            Assert.IsTrue(array[0].SequenceEqual(new int[] { 1, 2, 3 }));
            Assert.IsTrue(array[1].SequenceEqual(new int[] { 4, 5, 6 }));
            Assert.IsTrue(array[2].SequenceEqual(new int[] { 7, 8, 9 }));
        }

        [TestMethod]
        public void UnevenSegmentExample()
        {
            var items = new int[]
            {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
            };

            var chunked = items.ToSegments(4);
            Assert.IsTrue(chunked.Count() == 3);
            Assert.IsTrue(chunked.All(c => c.Count() <= 4));

            var array = chunked.ToArray();
            Assert.IsTrue(array[0].SequenceEqual(new int[] { 1, 2, 3, 4 }));
            Assert.IsTrue(array[1].SequenceEqual(new int[] { 5, 6, 7, 8 }));
            Assert.IsTrue(array[2].SequenceEqual(new int[] { 9, 10, 11 }));
        }

        [TestMethod]
        public void TwoBigSegments()
        {
            var items = Enumerable.Range(1, 100);
            var chunked = items.ToSegments(50);
            Assert.IsTrue(chunked.Count() == 2);
            Assert.IsTrue(chunked.All(c => c.Count() == 50));
        }

        [TestMethod]
        public void TwoBigSegmentssWithOneRemainder()
        {
            var items = Enumerable.Range(1, 101);
            var chunked = items.ToSegments(50);
            Assert.IsTrue(chunked.Count() == 3);

            var array = chunked.ToArray();
            Assert.IsTrue(array[0].Count() == 50);
            Assert.IsTrue(array[1].Count() == 50);
            Assert.IsTrue(array[2].Count() == 1);
        }

    }
}
