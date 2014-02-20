package net.openhft.collections;

import net.openhft.lang.LongHashable;
import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;

public abstract class CharSequenceKey implements HugeMapKey<CharSequenceKey>, LongHashable {
	private static class ImmutableKey extends CharSequenceKey {
		private final String value;
		
		public ImmutableKey(String value) {
			this.value = value;
		}
		
		public String getValue() { return value; }
		
		@Override
		public CharSequenceKey createKeyForPut() {
			// Method not used on immutable key.
			throw new UnsupportedOperationException();
		}

		@Override
		public void set(CharSequence prefix, int key) {
			// Method not used on immutable key.
			throw new UnsupportedOperationException();
		}

		@Override
		public void readMarshallable(Bytes in) throws IllegalStateException {
			// Method not used on immutable key.
			throw new UnsupportedOperationException();
		}
	}
	
	private static class RecyclableKey extends CharSequenceKey {
		private final StringBuilder builder = new StringBuilder();
		
		@Override
		public CharSequenceKey createKeyForPut() {
			return new ImmutableKey(builder.toString());
		}

		@Override
		protected CharSequence getValue() {
			return builder;
		}
		
		@Override
		public void set(CharSequence prefix, int key) {
			builder.setLength(0);
			builder.append(prefix);
			builder.append(key);
		}
		
		@Override
		public void readMarshallable(Bytes in) throws IllegalStateException {
			builder.setLength(0);
			in.readUTFΔ(builder);
		}		
	}
	
	public static  CharSequenceKey createRecyclableKey() {
		return new RecyclableKey();
	}

	@Override
	public abstract void readMarshallable(Bytes in) throws IllegalStateException;

	@Override
	public void writeMarshallable(Bytes out) {
		out.writeUTFΔ(getValue());
	}
	
	public abstract void set(CharSequence prefix, int key);
	
	protected abstract CharSequence getValue();
	
	@Override
	public long longHashCode() {
		return Maths.hash(getValue());
	}
	
	@Override
	public int hashCode() {
		return (int)longHashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof CharSequenceKey)) return false;
		CharSequenceKey key2 = (CharSequenceKey)obj;
		if (getValue().length() != key2.getValue().length())
             return false;
        for (int i = 0; i < getValue().length(); i++)
             if (getValue().charAt(i) != key2.getValue().charAt(i))
                 return false;
        return true;
	}
}
