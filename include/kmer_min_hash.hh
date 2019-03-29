#ifndef KMER_MIN_HASH_HH
#define KMER_MIN_HASH_HH

#include <algorithm>
#include <set>
#include <map>
#include <memory>
#include <queue>
#include <exception>
#include <string>

namespace sourmash {

extern "C" {
  #include "sourmash.h"
}

uint64_t _hash_murmur(const std::string& kmer, const uint32_t seed) {
  return hash_murmur(kmer.c_str(), seed);
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

void process_errors() {
	auto err_code = sourmash_err_get_last_code();

	switch (err_code) {
		case SOURMASH_ERROR_CODE_MISMATCH_K_SIZES:
			throw minhash_exception("different ksizes cannot be compared");
			break;
		case SOURMASH_ERROR_CODE_MISMATCH_D_N_A_PROT:
			throw minhash_exception("DNA/prot minhashes cannot be compared");
			break;
		case SOURMASH_ERROR_CODE_MISMATCH_MAX_HASH:
			throw minhash_exception("mismatch in max_hash; comparison fail");
			break;
		case SOURMASH_ERROR_CODE_MISMATCH_SEED:
			throw minhash_exception("mismatch in seed; comparison fail");
			break;
		default: break;
	}
}

class KmerMinHash
{
  protected:
    KmerMinHash* _this;

  public:
    KmerMinHash(unsigned int n, unsigned int k, bool prot, uint32_t s,
                HashIntoType mx) {
      _this = kmerminhash_new(n, k, prot, s, mx, false);
    };

    void add_hash(const HashIntoType h) {
      kmerminhash_add_hash(_this, h);
    }

    void remove_hash(const HashIntoType h) {
      kmerminhash_remove_hash(_this, h);
    }

    void add_word(const std::string& word) {
      kmerminhash_add_word(_this, word.c_str());
    }

    void add_sequence(const char * sequence, bool force=false) {
      kmerminhash_add_sequence(_this, sequence, force);
      process_errors();
    }

    void merge(const KmerMinHash& other) {
      kmerminhash_merge(_this, other._this);
      process_errors();
    }

    unsigned int count_common(const KmerMinHash& other) {
      auto v = kmerminhash_count_common(_this, other._this);
      process_errors();
      return v;
    }

    size_t size() {
      return kmerminhash_get_mins_size(_this);
    }

    uint32_t num() { return kmerminhash_num(_this); }

    uint64_t seed() { return kmerminhash_seed(_this); }
    
    bool track_abundance() { return kmerminhash_track_abundance(_this); }

    bool is_protein() { return kmerminhash_is_protein(_this); }

    uint32_t ksize() { return kmerminhash_ksize(_this); }

    uint64_t max_hash() { return kmerminhash_max_hash(_this); }

    std::vector<HashIntoType> mins() {
      auto ptr = kmerminhash_get_mins(_this);
      std::vector<HashIntoType> m(ptr, ptr + kmerminhash_get_mins_size(_this));
      return m;
    }

    void set_abundances(std::vector<HashIntoType> mins, std::vector<HashIntoType> abunds) {
      auto max_h = max_hash();
      auto n = num();
      auto min_it = mins.begin();
      auto last_min = mins.end();
      auto abund_it = abunds.begin();

      if (track_abundance()) {
        size_t added = 0;
        for (; min_it != last_min; ++min_it, ++abund_it) {
          if (!max_h or *min_it <= max_h) {
            kmerminhash_mins_push(_this, *min_it);
            kmerminhash_abunds_push(_this, *abund_it);
            added += 1;
            if (n > 0 and added >= n) {
              break;
            }
          }
        }
      }
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

    std::vector<HashIntoType> abunds() {
      auto ptr = kmerminhash_get_abunds(_this);
      std::vector<HashIntoType> m(ptr, ptr + kmerminhash_get_abunds_size(_this));
      return m;
    }

    ~KmerMinAbundance() throw() {}
};
}

#endif // KMER_MIN_HASH_HH
