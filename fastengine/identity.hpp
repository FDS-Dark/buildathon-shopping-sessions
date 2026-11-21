#pragma once
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

struct IdentityMap {
  struct Name { uint64_t packed=0; uint64_t dense=UINT64_MAX; uint64_t lex=0; std::string generic; bool is_packed=false; };
  std::vector<uint32_t> parent, size, min_node;
  std::vector<Name> names;
  std::unordered_map<uint64_t,uint32_t> packed_nodes;
  std::unordered_map<std::string,uint32_t> generic_nodes;
  std::vector<uint32_t> linked_small;
  std::unordered_map<uint64_t,uint32_t> linked_extra;
  uint64_t accepted=0, malformed=0;
  bool domain_failed=false;

  uint32_t find(uint32_t x) { uint32_t r=x; while(parent[r]!=r) r=parent[r]; while(parent[x]!=x){uint32_t n=parent[x];parent[x]=r;x=n;} return r; }
  static std::string packed_bytes(uint64_t x) { unsigned n=unsigned((x>>52)&15); uint64_t v=x&((1ULL<<48)-1); std::string s(n+1,'0'); s[0]=char(x>>56); for(unsigned i=0;i<n;++i){unsigned d=unsigned(v%36);v/=36;s[n-i]=char(d<10?'0'+d:'a'+d-10);} return s; }
  std::string bytes(uint32_t n) const { return names[n].is_packed?packed_bytes(names[n].packed):names[n].generic; }
  static std::string_view packed_view(uint64_t x,char (&buf)[13]) { unsigned n=unsigned((x>>52)&15);uint64_t v=x&((1ULL<<48)-1);buf[0]=char(x>>56);for(unsigned i=0;i<n;++i){unsigned d=unsigned(v%36);v/=36;buf[n-i]=char(d<10?'0'+d:'a'+d-10);}return {buf,n+1}; }
  bool less(uint32_t a,uint32_t b) const { Name const&x=names[a],&y=names[b];if(x.is_packed&&y.is_packed)return x.lex<y.lex;if(!x.is_packed&&!y.is_packed)return x.generic<y.generic;char xb[13],yb[13];std::string_view xv=x.is_packed?packed_view(x.packed,xb):std::string_view(x.generic);std::string_view yv=y.is_packed?packed_view(y.packed,yb):std::string_view(y.generic);return xv<yv; }
  uint32_t node(uint64_t p,std::string_view raw,bool common) {
    if(common){auto [it,ins]=packed_nodes.emplace(p,uint32_t(names.size()));if(!ins)return it->second;uint64_t lex=0;for(size_t i=1;i<raw.size();++i)lex|=uint64_t(uint8_t(raw[i]))<<(64-8*i);names.push_back(Name{p,UINT64_MAX,lex,{},true});}
    else {auto [it,ins]=generic_nodes.emplace(std::string(raw),uint32_t(names.size()));if(!ins)return it->second;names.push_back(Name{0,UINT64_MAX,0,it->first,false});}
    uint32_t id=uint32_t(names.size()-1);parent.push_back(id);size.push_back(1);min_node.push_back(id);return id;
  }
  void unite(uint32_t a,uint32_t b){a=find(a);b=find(b);if(a==b)return;if(size[a]<size[b])std::swap(a,b);parent[b]=a;size[a]+=size[b];if(less(min_node[b],min_node[a]))min_node[a]=min_node[b];}
  static bool split2(std::string_view line,std::string_view&a,std::string_view&b){if(line.empty())return false;size_t c=line.find(',');if(c==line.npos||line.find(',',c+1)!=line.npos)return false;auto f=[](std::string_view x,std::string_view&o){if(!x.empty()&&x.front()=='"'){if(x.size()<2||x.back()!='"')return false;x.remove_prefix(1);x.remove_suffix(1);if(x.find('"')!=x.npos)return false;}else if(x.find('"')!=x.npos)return false;o=x;return true;};return f(line.substr(0,c),a)&&f(line.substr(c+1),b);}

  template<class PP,class DN,class SN> static IdentityMap build(std::filesystem::path const&dir,uint64_t key,bool key_ok,uint32_t persons,PP parse_packed,DN decode,SN scramble){
    IdentityMap o;
    o.packed_nodes.reserve(key_ok?1024:size_t(persons)*2);
    size_t expected_nodes=size_t(persons)*3/2+1024;
    o.names.reserve(expected_nodes);o.parent.reserve(expected_nodes);o.size.reserve(expected_nodes);o.min_node.reserve(expected_nodes);
    std::vector<uint32_t> direct(key_ok?size_t(persons)*9:0,std::numeric_limits<uint32_t>::max());
    std::unordered_map<uint64_t,uint32_t> dense_extra; dense_extra.reserve(size_t(persons)/8+64);
    auto fresh_packed=[&](uint64_t p,uint64_t q,std::string_view raw){uint64_t lex=0;for(size_t i=1;i<raw.size();++i)lex|=uint64_t(uint8_t(raw[i]))<<(64-8*i);uint32_t id=uint32_t(o.names.size());o.names.push_back(Name{p,q,lex,{},true});o.parent.push_back(id);o.size.push_back(1);o.min_node.push_back(id);return id;};
    auto endpoint=[&](std::string_view raw){uint64_t p=0;if(!parse_packed(raw,'u',p))return o.node(0,raw,false);if(!key_ok)return o.node(p,raw,true);uint64_t num=p&((1ULL<<48)-1),q=decode(num,key);bool ordinary=q<uint64_t(persons)*512,kiosk=q>=(1ULL<<39)&&q<(1ULL<<39)+256;if(!ordinary&&!kiosk){o.domain_failed=true;return o.node(p,raw,true);}if(ordinary&&(q&511)<9){uint32_t&slot=direct[size_t(q>>9)*9+(q&511)];if(slot==UINT32_MAX)slot=fresh_packed(p,q,raw);return slot;}auto[it,ins]=dense_extra.emplace(q,uint32_t(o.names.size()));if(ins)fresh_packed(p,q,raw);return it->second;};
    std::vector<std::filesystem::path> ps;if(std::filesystem::exists(dir))for(auto const&e:std::filesystem::directory_iterator(dir))if(e.is_regular_file()&&e.path().extension()==".csv")ps.push_back(e.path());std::sort(ps.begin(),ps.end());
    for(auto const&p:ps){std::ifstream in(p,std::ios::binary);std::string line;std::getline(in,line);while(std::getline(in,line)){std::string_view a,b;if(!split2(line,a,b)||a.empty()||b.empty()){++o.malformed;continue;}o.unite(endpoint(a),endpoint(b));++o.accepted;}}
    for(uint32_t i=0;i<o.parent.size();++i)o.parent[i]=o.find(i);
    if(key_ok){o.linked_small=std::move(direct);for(uint32_t&n:o.linked_small)if(n!=UINT32_MAX)n=o.parent[n];o.linked_extra.reserve(dense_extra.size()*2);for(auto const&[q,n]:dense_extra)o.linked_extra.emplace(q,o.parent[n]);}
    return o;
  }
  uint32_t resolve_dense(uint64_t q)const{uint64_t p=q>>9,a=q&511;if(a<9&&size_t(p)*9+a<linked_small.size())return linked_small[size_t(p)*9+a];auto it=linked_extra.find(q);return it==linked_extra.end()?std::numeric_limits<uint32_t>::max():it->second;}
};

