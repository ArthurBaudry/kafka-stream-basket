namespace java prices
namespace js prices

typedef i64 long // We can use typedef to get pretty names for the types we are using

service KakfaStateService
{
        map<string,long> getAll(1:string store),
}