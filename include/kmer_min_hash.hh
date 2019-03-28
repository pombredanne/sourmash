#ifndef KMER_MIN_HASH_HH
#define KMER_MIN_HASH_HH

#include <algorithm>
#include <set>
#include <map>
#include <memory>
#include <queue>
#include <exception>
#include <string>

extern "C" {
  #include "sourmash.h"
}

typedef uint64_t HashIntoType;

class minhash_exception : public std::exception
{
public:
    explicit minhash_exception(const std::string& msg = "Generic minhash exception")
        : _msg(msg) { }

    virtual ~minhash_exception() throw() { }
    virtual const char* what() const throw ()
    {
        return _msg.c_str();
    }

protected:
    const std::string _msg;
};

class KmerMinHash
{
  protected:
    KmerMinHash* _this;

  public:
    KmerMinHash(unsigned int n, unsigned int k, bool prot, uint32_t s,
                HashIntoType mx) {
      _this = kmerminhash_new(n, k, prot, s, mx, false);
    };

    void check_compatible(const KmerMinHash& other) {
    }

    void add_hash(const HashIntoType h) {
      return kmerminhash_add_hash(_this, h);
    }

    void remove_hash(const HashIntoType h) {
    }

    void add_word(const std::string& word) {
      kmerminhash_add_word(_this, word.c_str());
    }

    void add_sequence(const char * sequence, bool force=false) {
      kmerminhash_add_sequence(_this, sequence, force);
    }

    void merge(const KmerMinHash& other) {
      kmerminhash_merge(_this, other._this);
    }

    unsigned int count_common(const KmerMinHash& other) {
      kmerminhash_count_common(_this, other._this);
    }

    size_t size() {
      return kmerminhash_get_mins_size(_this);
    }

    ~KmerMinHash() throw() {
      kmerminhash_free(_this);
    }
};

class KmerMinAbundance: public KmerMinHash {
 public:
    KmerMinAbundance(unsigned int n, unsigned int k, bool prot, uint32_t seed,
                     HashIntoType mx) : KmerMinHash(n, k, prot, seed, mx) {
      kmerminhash_free(_this);
      _this = kmerminhash_new(n, k, prot, seed, mx, true);
    };

    ~KmerMinAbundance() throw() {}
};

#endif // KMER_MIN_HASH_HH
