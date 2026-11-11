# Loads the patch of Rdkafka::Producer#close for the installed rdkafka
# version. out_rdkafka and out_rdkafka2 both require this file, so the same
# patch is used whichever of them is loaded first.
begin
  rdkafka_version = Gem::Version::create(Rdkafka::VERSION)
  if rdkafka_version >= Gem::Version.create('0.16.0')
    require_relative 'rdkafka_patch/0_16_0'
  elsif rdkafka_version >= Gem::Version.create('0.14.0')
    require_relative 'rdkafka_patch/0_14_0'
  elsif rdkafka_version >= Gem::Version.create('0.13.0')
    require_relative 'rdkafka_patch/0_13_0'
  elsif rdkafka_version >= Gem::Version.create('0.12.0')
    require_relative 'rdkafka_patch/0_12_0'
  else
    require_relative 'rdkafka_patch/0_11_0'
  end
rescue LoadError, NameError
  raise "unable to patch rdkafka."
end
