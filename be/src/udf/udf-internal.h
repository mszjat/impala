// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string.h>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

/// Be very careful when adding Impala includes in this file. We don't want to pull
/// in unnecessary dependencies for the development libs.
#include "udf/udf.h"

namespace impala {

#define RETURN_IF_NULL(ctx, ptr)                            \
  do {                                                      \
    if (UNLIKELY(ptr == NULL)) {                            \
      DCHECK(!ctx->impl()->state()->GetQueryStatus().ok()); \
      return;                                               \
    }                                                       \
  } while (false)

class FreePool;
class MemPool;
class RuntimeState;
class ScalarExpr;

/// This class actually implements the interface of FunctionContext. This is split to
/// hide the details from the external header.
/// Note: The actual user code does not include this file.
///
/// Exprs (e.g. UDFs and UDAs) require a FunctionContext to store state related to
/// evaluation of the expression. Each FunctionContext is associated with a backend Expr
/// or AggFnEvaluator, which is derived from a TExprNode generated by the Impala frontend.
/// FunctionContexts are allocated and managed by ScalarExprEvaluator. Exprs shouldn't try
/// to create FunctionContext themselves.
class FunctionContextImpl {
 public:
  /// Create a FunctionContext for a UDF. Caller is responsible for deleting it.
  /// UDF-managed allocations (i.e. Allocate()) are backed by 'perm_pool' and
  /// allocations that may hold expr results (i.e. AllocateForResults()) are backed
  /// by 'results_pool'.
  static impala_udf::FunctionContext* CreateContext(RuntimeState* state,
      MemPool* perm_pool, MemPool* results_pool,
      const impala_udf::FunctionContext::TypeDesc& return_type,
      const std::vector<impala_udf::FunctionContext::TypeDesc>& arg_types,
      int varargs_buffer_size = 0, bool debug = false);

  /// Create a FunctionContext for a UDA. Identical to the UDF version except for the
  /// intermediate type. Caller is responsible for deleting it.
  static impala_udf::FunctionContext* CreateContext(RuntimeState* state,
      MemPool* perm_pool, MemPool* results_pool,
      const impala_udf::FunctionContext::TypeDesc& intermediate_type,
      const impala_udf::FunctionContext::TypeDesc& return_type,
      const std::vector<impala_udf::FunctionContext::TypeDesc>& arg_types,
      int varargs_buffer_size = 0, bool debug = false);

  FunctionContextImpl(impala_udf::FunctionContext* parent);
  ~FunctionContextImpl();

  /// Checks for any outstanding memory allocations. If there is (non-result) memory that
  /// was allocated by the UDF via this FunctionContext but not freed, adds a warning
  /// and frees the allocations.
  void Close();

  /// Returns a new FunctionContext with the same constant args, fragment-local state, and
  /// debug flag as this FunctionContext. The caller is responsible for calling delete on
  /// it. The cloned FunctionContext cannot be used after the original FunctionContext is
  /// destroyed because it may reference fragment-local state from the original.
  impala_udf::FunctionContext* Clone(MemPool* perm_pool, MemPool* results_pool);

  /// Allocates a buffer of 'byte_size' to hold expr results. If the new allocation
  /// causes the memory limit to be exceeded, the error will be set in this object
  /// causing the query to fail.
  ///
  /// These allocations live in the 'results_pool' passed into the constructor.
  /// 'results_pool' is managed by the Impala runtime and can be safely cleared
  /// whenever memory returned by the expression is no longer referenced.
  uint8_t* AllocateForResults(int64_t byte_size) noexcept;

  /// Replaces the current 'results_pool_' for  'new_results_pool' to be used for
  /// AllocateForResults(). Returns a pointer to the pool that was replaced.
  MemPool* SwapResultsPool(MemPool* new_results_pool) {
    MemPool* old_results_pool = results_pool_;
    results_pool_ = new_results_pool;
    return old_results_pool;
  }

  /// Sets the constant arg list. The vector should contain one entry per argument,
  /// with a non-NULL entry if the argument is constant. The AnyVal* values are
  /// owned by the caller and must be allocated from the ScalarExprEvaluator's MemPool.
  void SetConstantArgs(std::vector<impala_udf::AnyVal*>&& constant_args);

  typedef std::vector<std::pair<ScalarExpr*, impala_udf::AnyVal*>> NonConstantArgsVector;

  /// Sets the non-constant args. Contains one entry per non-constant argument. All
  /// pointers should be non-NULL. The Expr* and AnyVal* values are owned by the caller.
  /// The AnyVal* values must be allocated from the ScalarExprEvaluator's MemPool.
  void SetNonConstantArgs(NonConstantArgsVector&& non_constant_args);

  const std::vector<impala_udf::AnyVal*>& constant_args() const { return constant_args_; }
  const NonConstantArgsVector& non_constant_args() const { return non_constant_args_; }

  uint8_t* varargs_buffer() { return varargs_buffer_; }

  std::vector<impala_udf::AnyVal*>* staging_input_vals() { return &staging_input_vals_; }

  bool debug() { return debug_; }
  bool closed() { return closed_; }

  int64_t num_updates() const { return num_updates_; }
  int64_t num_removes() const { return num_removes_; }
  void set_num_updates(int64_t n) { num_updates_ = n; }
  void set_num_removes(int64_t n) { num_removes_ = n; }
  void IncrementNumUpdates(int64_t n = 1) { num_updates_ += n; }
  void IncrementNumRemoves(int64_t n = 1) { num_removes_ += n; }

  const std::vector<impala_udf::FunctionContext::TypeDesc> arg_types() {
    return arg_types_;
  }

  RuntimeState* state() { return state_; }

  /// Various static attributes of the UDF/UDA that can be injected as constants
  /// by codegen. Note that the argument types refer to those in the UDF/UDA signature,
  /// not the arguments of the C++ functions implementing the UDF/UDA. Any change to
  /// this enum must be reflected in FunctionContextImpl::GetConstFnAttr().
  enum ConstFnAttr {
    /// RETURN_TYPE_*: properties of FunctionContext::GetReturnType()
    RETURN_TYPE_SIZE, // int
    RETURN_TYPE_PRECISION, // int
    RETURN_TYPE_SCALE, // int
    /// ARG_TYPE_* with parameter i: properties of FunctionContext::GetArgType(i)
    ARG_TYPE_SIZE, // int[]
    ARG_TYPE_PRECISION, // int[]
    ARG_TYPE_SCALE, // int[]
    /// True if decimal_v2 query option is set.
    DECIMAL_V2,
    /// True if utf8_mode query option is set.
    UTF8_MODE,
  };

  /// This function returns the various static attributes of the UDF/UDA. Calls to this
  /// function are replaced by constants injected by codegen. If codegen is disabled,
  /// this function is interpreted as-is.
  ///
  /// 't' is the static function attribute defined in the ConstFnAttr enum above.
  /// For function attributes of arguments, 'i' holds the argument number (0 indexed).
  /// Please note that argument refers to the arguments in the signature of the UDF or UDA.
  /// 'i' must always be an immediate integer value in order to utilize the constant
  /// replacement when codegen is enabled. e.g., it cannot be a variable or an expression
  /// like "1 + 1".
  ///
  int GetConstFnAttr(ConstFnAttr t, int i = -1);

  /// Return the function attribute 't' defined in ConstFnAttr above.
  static int GetConstFnAttr(bool uses_decimal_v2, bool is_utf8_mode,
      const impala_udf::FunctionContext::TypeDesc& return_type,
      const std::vector<impala_udf::FunctionContext::TypeDesc>& arg_types, ConstFnAttr t,
      int i = -1);

  /// UDFs may manipulate DecimalVal arguments via SIMD instructions such as 'movaps'
  /// that require 16-byte memory alignment.
  static const int VARARGS_BUFFER_ALIGNMENT = 16;

  /// The LLVM class name for FunctionContext. Used for handcrafted IR.
  static const char* LLVM_FUNCTIONCONTEXT_NAME;

  /// FunctionContextImpl::GetConstFnAttr() symbol. Used for call sites replacement.
  static const char* GET_CONST_FN_ATTR_SYMBOL;

 private:
  friend class impala_udf::FunctionContext;
  friend class ScalarExprEvaluator;

  /// A utility function which checks for memory limits and null pointers returned by
  /// Allocate(), Reallocate() and AllocateForResults() and sets the appropriate error status
  /// if necessary.
  ///
  /// Return false if 'buf' is null; returns true otherwise.
  bool CheckAllocResult(const char* fn_name, uint8_t* buf, int64_t byte_size);

  /// A utility function which checks for memory limits that may have been exceeded by
  /// Allocate(), Reallocate(), AllocateForResults() or TrackAllocation(). Sets the
  /// appropriate error status if necessary.
  void CheckMemLimit(const char* fn_name, int64_t byte_size);

  /// Preallocated buffer for storing varargs (if the function has any). Allocated and
  /// owned by this object, but populated by an Expr function. The buffer is interpreted
  /// as an array of the appropriate AnyVal subclass.
  uint8_t* varargs_buffer_;
  int varargs_buffer_size_;

  /// Parent context object. Not owned
  impala_udf::FunctionContext* context_;

  /// Pool used for allocations made via Allocate(). Allocations are explicitly freed and
  /// returned to this pool with Free(). The memory allocated in this pool is effectively
  /// owned by the UDF.
  /// Owned and freed in destructor. Uses raw pointer to avoid pulling headers into SDK.
  FreePool* udf_pool_;

  /// Pool used for allocations made via AllocateForResults(). Not owned by this
  /// FunctionContext. Allocations made from the pool are used temporarily during
  /// expression evaluation. Var-len values returned from an expression may reference
  /// memory in this pool - the caller is responsible for ensuring that the pool is
  /// not cleared while that memory is still referenced.
  MemPool* results_pool_;

  /// We use the query's runtime state to report errors and warnings. NULL for test
  /// contexts.
  RuntimeState* state_;

  /// If true, indicates this is a debug context which will do additional validation.
  bool debug_;

  impala_udf::FunctionContext::ImpalaVersion version_;

  /// Empty if there's no error
  std::string error_msg_;

  /// The number of warnings reported.
  int64_t num_warnings_;

  /// The number of calls to Update()/Remove().
  int64_t num_updates_;
  int64_t num_removes_;

  /// Allocations made and still owned by the user function. Only used if debug_ is true
  /// because it is very expensive to maintain.
  std::map<uint8_t*, int> allocations_;

  /// The function state accessed via FunctionContext::Get/SetFunctionState()
  void* thread_local_fn_state_;
  void* fragment_local_fn_state_;

  /// The number of bytes allocated externally by the user function. In some cases,
  /// it is too inconvenient to use the Allocate()/Free() APIs in the FunctionContext,
  /// particularly for existing codebases (e.g. they use std::vector). Instead, they'll
  /// have to track those allocations manually.
  int64_t external_bytes_tracked_;

  /// Type descriptor for the intermediate type of a UDA. Set to INVALID_TYPE for UDFs.
  impala_udf::FunctionContext::TypeDesc intermediate_type_;

  /// Type descriptor for the return type of the function.
  impala_udf::FunctionContext::TypeDesc return_type_;

  /// Type descriptors for each argument of the function.
  std::vector<impala_udf::FunctionContext::TypeDesc> arg_types_;

  /// Contains an AnyVal* for each argument of the function. If the AnyVal* is NULL,
  /// indicates that the corresponding argument is non-constant. Otherwise contains the
  /// value of the argument. The AnyVal* objects and associated data are owned by the
  /// ScalarExprEvaluator provided when opening the FRAGMENT_LOCAL expression contexts.
  std::vector<impala_udf::AnyVal*> constant_args_;

  /// Vector of all non-constant children expressions that need to be evaluated for
  /// each input row. The first element of each pair is the child expression and the
  /// second element is the value it must be evaluated into.
  NonConstantArgsVector non_constant_args_;

  /// Used by ScalarFnCall to temporarily store arguments for a UDF when running without
  /// codegen. Allows us to pass AnyVal* arguments to the scalar function directly,
  /// rather than codegening a call that passes the correct AnyVal subclass pointer type.
  /// Note that this is only used for non-variadic arguments; varargs are always stored
  /// in varargs_buffer_.
  std::vector<impala_udf::AnyVal*> staging_input_vals_;

  /// Indicates whether this context has been closed. Used for verification/debugging.
  bool closed_;
};

}

namespace impala_udf {

/// Temporary CollectionVal definition, used to represent arrays and maps. This is not
/// ready for public consumption because users must have access to our internal tuple
/// layout.
struct CollectionVal : public AnyVal {
  // Put num_tuples before ptr so that 'AnyVal::is_null', 'num_tuples' and 'ptr' can be
  // packed into 16 bytes. This matches the memory layout of StringVal, which allows
  // sharing of support in CodegenAnyval.
  int num_tuples;
  uint8_t* ptr;

  /// Construct an CollectionVal from ptr/num_tuples. Note: this does not make a copy of
  /// ptr so the buffer must exist as long as this CollectionVal does.
  CollectionVal(uint8_t* ptr = NULL, int num_tuples = 0)
      : num_tuples(num_tuples), ptr(ptr) {}

  static CollectionVal null() {
    CollectionVal cv;
    cv.is_null = true;
    return cv;
  }
};

/// A struct is represented by a vector of pointers where these pointers point to the
/// children of the struct.
struct StructVal : public AnyVal {
  int num_children;

  /// Pointer to the start of the vector of children pointers. The set of types that the
  /// pointed-to values can have is types of the members of 'ExprValue'. A null pointer
  /// means that this child is NULL. The buffer is not null-terminated.
  /// Memory allocation to 'ptr' is done using FunctionContext. As a result it's not
  /// needed to take care of memory deallocation in StructVal as it will be done through
  /// FunctionContext automatically.
  uint8_t** ptr;

  StructVal() : AnyVal(true), num_children(0), ptr(nullptr) {}

  StructVal(FunctionContext* ctx, int num_children) : AnyVal(),
      num_children(num_children), ptr(nullptr) {
    ReserveMemory(ctx);
  }

  static StructVal null() { return StructVal(); }

  void addChild(void* child, int idx) {
    assert(idx >= 0 && idx < num_children);
    ptr[idx] = (uint8_t*)child;
  }

private:
  /// Uses FunctionContext to reserve memory for 'num_children' number of pointers. Sets
  /// 'ptr' to the beginning of this allocated memory.
  void ReserveMemory(FunctionContext* ctx);
};

#pragma GCC diagnostic ignored "-Winvalid-offsetof"
static_assert(sizeof(CollectionVal) == sizeof(StringVal), "Wrong size.");
static_assert(
    offsetof(CollectionVal, num_tuples) == offsetof(StringVal, len), "Wrong offset.");
static_assert(offsetof(CollectionVal, ptr) == offsetof(StringVal, ptr), "Wrong offset.");

static_assert(sizeof(StructVal) == sizeof(StringVal), "Wrong size.");
static_assert(
    offsetof(StructVal, num_children) == offsetof(StringVal, len), "Wrong offset.");
static_assert(offsetof(StructVal, ptr) == offsetof(StringVal, ptr), "Wrong offset.");
} // namespace impala_udf
