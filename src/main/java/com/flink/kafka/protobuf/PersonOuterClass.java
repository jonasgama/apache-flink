// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: person.proto

package com.flink.kafka.protobuf;

public final class PersonOuterClass {
  private PersonOuterClass() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface PersonOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.flink.kafka.protobuf.Person)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>string name = 1;</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>string name = 1;</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>int32 id = 2;</code>
     * @return The id.
     */
    int getId();

    /**
     * <code>string email = 3;</code>
     * @return The email.
     */
    java.lang.String getEmail();
    /**
     * <code>string email = 3;</code>
     * @return The bytes for email.
     */
    com.google.protobuf.ByteString
        getEmailBytes();

    /**
     * <code>string secondEmail = 4;</code>
     * @return The secondEmail.
     */
    java.lang.String getSecondEmail();
    /**
     * <code>string secondEmail = 4;</code>
     * @return The bytes for secondEmail.
     */
    com.google.protobuf.ByteString
        getSecondEmailBytes();

    /**
     * <code>string lastName = 5;</code>
     * @return The lastName.
     */
    java.lang.String getLastName();
    /**
     * <code>string lastName = 5;</code>
     * @return The bytes for lastName.
     */
    com.google.protobuf.ByteString
        getLastNameBytes();
  }
  /**
   * Protobuf type {@code com.flink.kafka.protobuf.Person}
   */
  public static final class Person extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:com.flink.kafka.protobuf.Person)
      PersonOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use Person.newBuilder() to construct.
    private Person(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private Person() {
      name_ = "";
      email_ = "";
      secondEmail_ = "";
      lastName_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new Person();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Person(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              java.lang.String s = input.readStringRequireUtf8();

              name_ = s;
              break;
            }
            case 16: {

              id_ = input.readInt32();
              break;
            }
            case 26: {
              java.lang.String s = input.readStringRequireUtf8();

              email_ = s;
              break;
            }
            case 34: {
              java.lang.String s = input.readStringRequireUtf8();

              secondEmail_ = s;
              break;
            }
            case 42: {
              java.lang.String s = input.readStringRequireUtf8();

              lastName_ = s;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.flink.kafka.protobuf.PersonOuterClass.internal_static_com_flink_kafka_protobuf_Person_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.flink.kafka.protobuf.PersonOuterClass.internal_static_com_flink_kafka_protobuf_Person_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.flink.kafka.protobuf.PersonOuterClass.Person.class, com.flink.kafka.protobuf.PersonOuterClass.Person.Builder.class);
    }

    public static final int NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object name_;
    /**
     * <code>string name = 1;</code>
     * @return The name.
     */
    @java.lang.Override
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      }
    }
    /**
     * <code>string name = 1;</code>
     * @return The bytes for name.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int ID_FIELD_NUMBER = 2;
    private int id_;
    /**
     * <code>int32 id = 2;</code>
     * @return The id.
     */
    @java.lang.Override
    public int getId() {
      return id_;
    }

    public static final int EMAIL_FIELD_NUMBER = 3;
    private volatile java.lang.Object email_;
    /**
     * <code>string email = 3;</code>
     * @return The email.
     */
    @java.lang.Override
    public java.lang.String getEmail() {
      java.lang.Object ref = email_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        email_ = s;
        return s;
      }
    }
    /**
     * <code>string email = 3;</code>
     * @return The bytes for email.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getEmailBytes() {
      java.lang.Object ref = email_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        email_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int SECONDEMAIL_FIELD_NUMBER = 4;
    private volatile java.lang.Object secondEmail_;
    /**
     * <code>string secondEmail = 4;</code>
     * @return The secondEmail.
     */
    @java.lang.Override
    public java.lang.String getSecondEmail() {
      java.lang.Object ref = secondEmail_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        secondEmail_ = s;
        return s;
      }
    }
    /**
     * <code>string secondEmail = 4;</code>
     * @return The bytes for secondEmail.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getSecondEmailBytes() {
      java.lang.Object ref = secondEmail_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        secondEmail_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int LASTNAME_FIELD_NUMBER = 5;
    private volatile java.lang.Object lastName_;
    /**
     * <code>string lastName = 5;</code>
     * @return The lastName.
     */
    @java.lang.Override
    public java.lang.String getLastName() {
      java.lang.Object ref = lastName_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        lastName_ = s;
        return s;
      }
    }
    /**
     * <code>string lastName = 5;</code>
     * @return The bytes for lastName.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getLastNameBytes() {
      java.lang.Object ref = lastName_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        lastName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
      }
      if (id_ != 0) {
        output.writeInt32(2, id_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(email_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 3, email_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(secondEmail_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 4, secondEmail_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(lastName_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 5, lastName_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(name_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
      }
      if (id_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, id_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(email_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, email_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(secondEmail_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, secondEmail_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(lastName_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(5, lastName_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.flink.kafka.protobuf.PersonOuterClass.Person)) {
        return super.equals(obj);
      }
      com.flink.kafka.protobuf.PersonOuterClass.Person other = (com.flink.kafka.protobuf.PersonOuterClass.Person) obj;

      if (!getName()
          .equals(other.getName())) return false;
      if (getId()
          != other.getId()) return false;
      if (!getEmail()
          .equals(other.getEmail())) return false;
      if (!getSecondEmail()
          .equals(other.getSecondEmail())) return false;
      if (!getLastName()
          .equals(other.getLastName())) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + NAME_FIELD_NUMBER;
      hash = (53 * hash) + getName().hashCode();
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + getId();
      hash = (37 * hash) + EMAIL_FIELD_NUMBER;
      hash = (53 * hash) + getEmail().hashCode();
      hash = (37 * hash) + SECONDEMAIL_FIELD_NUMBER;
      hash = (53 * hash) + getSecondEmail().hashCode();
      hash = (37 * hash) + LASTNAME_FIELD_NUMBER;
      hash = (53 * hash) + getLastName().hashCode();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.flink.kafka.protobuf.PersonOuterClass.Person parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.flink.kafka.protobuf.PersonOuterClass.Person prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.flink.kafka.protobuf.Person}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.flink.kafka.protobuf.Person)
        com.flink.kafka.protobuf.PersonOuterClass.PersonOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.flink.kafka.protobuf.PersonOuterClass.internal_static_com_flink_kafka_protobuf_Person_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.flink.kafka.protobuf.PersonOuterClass.internal_static_com_flink_kafka_protobuf_Person_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.flink.kafka.protobuf.PersonOuterClass.Person.class, com.flink.kafka.protobuf.PersonOuterClass.Person.Builder.class);
      }

      // Construct using com.flink.kafka.protobuf.PersonOuterClass.Person.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        name_ = "";

        id_ = 0;

        email_ = "";

        secondEmail_ = "";

        lastName_ = "";

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.flink.kafka.protobuf.PersonOuterClass.internal_static_com_flink_kafka_protobuf_Person_descriptor;
      }

      @java.lang.Override
      public com.flink.kafka.protobuf.PersonOuterClass.Person getDefaultInstanceForType() {
        return com.flink.kafka.protobuf.PersonOuterClass.Person.getDefaultInstance();
      }

      @java.lang.Override
      public com.flink.kafka.protobuf.PersonOuterClass.Person build() {
        com.flink.kafka.protobuf.PersonOuterClass.Person result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public com.flink.kafka.protobuf.PersonOuterClass.Person buildPartial() {
        com.flink.kafka.protobuf.PersonOuterClass.Person result = new com.flink.kafka.protobuf.PersonOuterClass.Person(this);
        result.name_ = name_;
        result.id_ = id_;
        result.email_ = email_;
        result.secondEmail_ = secondEmail_;
        result.lastName_ = lastName_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.flink.kafka.protobuf.PersonOuterClass.Person) {
          return mergeFrom((com.flink.kafka.protobuf.PersonOuterClass.Person)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.flink.kafka.protobuf.PersonOuterClass.Person other) {
        if (other == com.flink.kafka.protobuf.PersonOuterClass.Person.getDefaultInstance()) return this;
        if (!other.getName().isEmpty()) {
          name_ = other.name_;
          onChanged();
        }
        if (other.getId() != 0) {
          setId(other.getId());
        }
        if (!other.getEmail().isEmpty()) {
          email_ = other.email_;
          onChanged();
        }
        if (!other.getSecondEmail().isEmpty()) {
          secondEmail_ = other.secondEmail_;
          onChanged();
        }
        if (!other.getLastName().isEmpty()) {
          lastName_ = other.lastName_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.flink.kafka.protobuf.PersonOuterClass.Person parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.flink.kafka.protobuf.PersonOuterClass.Person) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>string name = 1;</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          name_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string name = 1;</code>
       * @return The bytes for name.
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string name = 1;</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string name = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>string name = 1;</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        name_ = value;
        onChanged();
        return this;
      }

      private int id_ ;
      /**
       * <code>int32 id = 2;</code>
       * @return The id.
       */
      @java.lang.Override
      public int getId() {
        return id_;
      }
      /**
       * <code>int32 id = 2;</code>
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(int value) {
        
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 id = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearId() {
        
        id_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object email_ = "";
      /**
       * <code>string email = 3;</code>
       * @return The email.
       */
      public java.lang.String getEmail() {
        java.lang.Object ref = email_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          email_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string email = 3;</code>
       * @return The bytes for email.
       */
      public com.google.protobuf.ByteString
          getEmailBytes() {
        java.lang.Object ref = email_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          email_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string email = 3;</code>
       * @param value The email to set.
       * @return This builder for chaining.
       */
      public Builder setEmail(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        email_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string email = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearEmail() {
        
        email_ = getDefaultInstance().getEmail();
        onChanged();
        return this;
      }
      /**
       * <code>string email = 3;</code>
       * @param value The bytes for email to set.
       * @return This builder for chaining.
       */
      public Builder setEmailBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        email_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object secondEmail_ = "";
      /**
       * <code>string secondEmail = 4;</code>
       * @return The secondEmail.
       */
      public java.lang.String getSecondEmail() {
        java.lang.Object ref = secondEmail_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          secondEmail_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string secondEmail = 4;</code>
       * @return The bytes for secondEmail.
       */
      public com.google.protobuf.ByteString
          getSecondEmailBytes() {
        java.lang.Object ref = secondEmail_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          secondEmail_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string secondEmail = 4;</code>
       * @param value The secondEmail to set.
       * @return This builder for chaining.
       */
      public Builder setSecondEmail(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        secondEmail_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string secondEmail = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearSecondEmail() {
        
        secondEmail_ = getDefaultInstance().getSecondEmail();
        onChanged();
        return this;
      }
      /**
       * <code>string secondEmail = 4;</code>
       * @param value The bytes for secondEmail to set.
       * @return This builder for chaining.
       */
      public Builder setSecondEmailBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        secondEmail_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object lastName_ = "";
      /**
       * <code>string lastName = 5;</code>
       * @return The lastName.
       */
      public java.lang.String getLastName() {
        java.lang.Object ref = lastName_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          lastName_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string lastName = 5;</code>
       * @return The bytes for lastName.
       */
      public com.google.protobuf.ByteString
          getLastNameBytes() {
        java.lang.Object ref = lastName_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          lastName_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string lastName = 5;</code>
       * @param value The lastName to set.
       * @return This builder for chaining.
       */
      public Builder setLastName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        lastName_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string lastName = 5;</code>
       * @return This builder for chaining.
       */
      public Builder clearLastName() {
        
        lastName_ = getDefaultInstance().getLastName();
        onChanged();
        return this;
      }
      /**
       * <code>string lastName = 5;</code>
       * @param value The bytes for lastName to set.
       * @return This builder for chaining.
       */
      public Builder setLastNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        lastName_ = value;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:com.flink.kafka.protobuf.Person)
    }

    // @@protoc_insertion_point(class_scope:com.flink.kafka.protobuf.Person)
    private static final com.flink.kafka.protobuf.PersonOuterClass.Person DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.flink.kafka.protobuf.PersonOuterClass.Person();
    }

    public static com.flink.kafka.protobuf.PersonOuterClass.Person getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Person>
        PARSER = new com.google.protobuf.AbstractParser<Person>() {
      @java.lang.Override
      public Person parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Person(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<Person> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Person> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public com.flink.kafka.protobuf.PersonOuterClass.Person getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_flink_kafka_protobuf_Person_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_com_flink_kafka_protobuf_Person_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\014person.proto\022\030com.flink.kafka.protobuf" +
      "\"X\n\006Person\022\014\n\004name\030\001 \001(\t\022\n\n\002id\030\002 \001(\005\022\r\n\005" +
      "email\030\003 \001(\t\022\023\n\013secondEmail\030\004 \001(\t\022\020\n\010last" +
      "Name\030\005 \001(\tb\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_com_flink_kafka_protobuf_Person_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_flink_kafka_protobuf_Person_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_com_flink_kafka_protobuf_Person_descriptor,
        new java.lang.String[] { "Name", "Id", "Email", "SecondEmail", "LastName", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
