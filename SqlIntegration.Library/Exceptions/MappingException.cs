using System;

namespace SqlIntegration.Library.Exceptions
{
    public class MappingException<TIdentity> : Exception
    {
        public MappingException(TIdentity sourceId, TIdentity newId, Exception innerException) : base($"Error mapping source Id {sourceId} to new Id {newId}: {innerException.Message}", innerException)
        {
            SourceId = sourceId;
            NewId = newId;
        }

        public TIdentity SourceId { get; }
        public TIdentity NewId { get; }
    }
}
