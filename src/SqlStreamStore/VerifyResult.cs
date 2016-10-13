namespace SqlStreamStore
{
    public class VerifyResult
    {
        public VerifyResult(bool isValid, string fault)
        {
            IsValid = isValid;
            Fault = fault;
        }

        public static VerifyResult Valid()
        {
            return new VerifyResult(true, string.Empty);
        }

        public static VerifyResult InValid(string fault)
        {
            return new VerifyResult(false, fault);
        }

        public string Fault { get; }

        public bool IsValid { get; }
    }
}